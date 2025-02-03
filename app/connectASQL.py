import os
import pyodbc, struct
import uvicorn
from azure import identity

from typing import Union, List
from fastapi import FastAPI
from pydantic import BaseModel
import requests

import time


class Track(BaseModel):
    track_name: str
    artist_name: str
    album_name: str
    track_uri: str


class SearchParam(BaseModel):
    track_name: str
    artist_name: str
    album_name: str


class UriParam(BaseModel):
    uri_list: List[str]


connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:musicdata.database.windows.net,1433;Database=spotifydata;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'

app = FastAPI()


@app.get("/")
def read_item():
    return {"message": "Welcome to our app"}


@app.get("/all")
def get_tracks(offset: int = 0):
    rows = []
    with get_conn() as conn:
        cursor = conn.cursor()
        qry = """SELECT * FROM [dbo].[tracks] ORDER BY (SELECT NULL) OFFSET ? ROWS FETCH NEXT 100 ROWS ONLY"""
        cursor.execute(qry, offset)

        for row in cursor.fetchall():
            rows.append(f"{row.track_name}, {row.artist_name}, {row.album_name}, {row.track_uri}")
    return rows


@app.get("/track/{track_id}")
def get_track(track_id: str):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM [dbo].[tracks] WHERE track_uri = ?", track_id)

        row = cursor.fetchone()
        return f"{row.track_name}, {row.artist_name}, {row.album_name}, {row.track_uri}"


@app.post("/uri")
def get_track_by_uri(uri_list_obj: UriParam):
    with get_conn() as conn:
        cursor = conn.cursor()
        placeholder = ",".join(f"'{uri}'" for uri in uri_list_obj.uri_list)
        qry = f"SELECT * FROM [dbo].[tracks] WHERE track_uri IN ({placeholder})"
        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        # print(qry)
        cursor.execute(qry)

        rows = []
        for row in cursor.fetchall():
            print(row.track_name, row.track_uri)
            rows.append(f"{row.track_name}, {row.artist_name}, {row.album_name}, {row.track_uri}")

        return rows


@app.get("/tracklike/{track_name}")
def get_track_like(track_name: str):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM [dbo].[tracks] WHERE track_name like '%" + track_name + "%'")
        rows = []
        for row in cursor.fetchall():
            print(row.track_name, row.track_uri)
            rows.append(f"{row.track_name}, {row.artist_name}, {row.album_name}, {row.track_uri}")

        return rows


@app.post("/tracksearch")
def get_track_search(search_params: SearchParam):
    if not search_params.track_name and not search_params.artist_name and not search_params.album_name:
        # if no search entry is made for any field then return empty row
        return []

    with get_conn() as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM [dbo].[tracks] WHERE 1=1"
        params = []

        if search_params.track_name:
            query += " AND track_name LIKE ?"
            params.append(f"%{search_params.track_name}%")
        if search_params.artist_name:
            query += " AND artist_name LIKE ?"
            params.append(f"%{search_params.artist_name}%")
        if search_params.album_name:
            query += " AND album_name LIKE ?"
            params.append(f"%{search_params.album_name}%")

        cursor.execute(query, params)
        rows = []
        for row in cursor.fetchall():
            rows.append(f"{row.track_name}, {row.artist_name}, {row.album_name}, {row.track_uri}")

        return rows

# @app.post("/person")
# def create_person(item: Person):
#     with get_conn() as conn:
#         cursor = conn.cursor()
#         cursor.execute(f"INSERT INTO Persons (FirstName, LastName) VALUES (?, ?)", item.first_name, item.last_name)
#         conn.commit()
#
#     return item


def get_conn():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=True)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

def run_job(uri_list):
    print("run_job invoked...")
    data = None
    print(type(uri_list))
    body = {"job_id": "623184021541739", "job_parameters": {"uri_list":",".join(uri_list)}}
    url = "https://adb-3895747841480133.13.azuredatabricks.net/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {os.environ['AZURE_DATABRICKS_PAT']}",
        "Content-Type": "json"
    }
    response = requests.post(url, headers=headers, json=body)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response content
        print(response.text)

        # Parse JSON response
        data = response.json()
    else:
        print(f"Run job request failed with status code: {response.status_code}")

    return data


def get_run_details(run_id):
    data = None
    url = f"https://adb-3895747841480133.13.azuredatabricks.net/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {os.environ['AZURE_DATABRICKS_PAT']}"
    }
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response content
        print(response.text)

        # Parse JSON response
        data = response.json()
    else:
        print(f"Get run details request failed with status code: {response.status_code}")

    return data

def get_run_output(task_id):
    data = None
    url = f"https://adb-3895747841480133.13.azuredatabricks.net/api/2.1/jobs/runs/get-output?run_id={task_id}"
    headers = {
        "Authorization": f"Bearer {os.environ['AZURE_DATABRICKS_PAT']}"
    }
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the response content
        print(response.text)

        # Parse JSON response
        data = response.json()
    else:
        print(f"Get run output request failed with status code: {response.status_code}")

    return data


@app.post("/get_recommendations")
def get_recommendations(uri_list_obj: UriParam):
    # run the job
    print(uri_list_obj)
    # job_run_response = run_job(uri_list_obj.uri_list)
    job_run_response = {"run_id": "35048021414416"}
    task_status = None
    task_detail = {}

    # get the run id and get the task id
    if job_run_response is not None:
        run_id = job_run_response["run_id"]
        details_response = get_run_details(run_id)
        if details_response is not None:
            task_id = details_response["tasks"][0]["run_id"]

            # check the status of job by polling every 30 seconds
            while task_status in [None, "PENDING", "RUNNING"]:
                # get task status
                task_detail = get_run_output(task_id)
                if task_detail is not None:
                    task_status = task_detail["metadata"]["state"]["life_cycle_state"]
                    if task_status == "TERMINATED":
                        break
                else:
                    print("Could not fetch task details, trying again...")
                time.sleep(30)
        else:
            print(f"Could not fetch run details. Reload the url and try again.")
    else:
        print(f"Could not run job. Check if you haven't spun a cluster accidentally.")

    if task_status == "TERMINATED":
        notebook_output = eval(task_detail["notebook_output"]["result"])
        return notebook_output
    else:
        return None