import streamlit as st
import pandas as pd
import json
import requests
from collections import OrderedDict
import time


HOSTNAME = "http://localhost:80/"

st.markdown("""
    <style>
    .stButton {
        margin-top: 29px;  /* Align with text input fields */
        height: 42px;      /* Match height of text inputs */
    }
    </style>
""", unsafe_allow_html=True)




if 'df_key' not in st.session_state:
    st.session_state.df_key = 0

if "retain_state" not in st.session_state:
    st.session_state["retain_state"] = False

def callbackfun():
    print("function called....")
    st.session_state["retain_state"] = True
    st.session_state["filtered_df"] = filtered_df
    if "input_songs" not in st.session_state:
        st.session_state["input_songs"] = OrderedDict()

    song_df_key = f"song_df_{st.session_state.df_key}"

    if len(filtered_df.iloc[st.session_state[song_df_key]["selection"]["rows"], 0].values) != 0:
        print(filtered_df.iloc[st.session_state[song_df_key]["selection"]["rows"]].values[0][-1])
        selected_song = filtered_df.iloc[st.session_state[song_df_key]["selection"]["rows"], 0].values[0]
        selected_song_uri = filtered_df.iloc[st.session_state[song_df_key]["selection"]["rows"]].values[0][-1]
        if selected_song not in st.session_state["input_songs"]:
            st.session_state["input_songs"][selected_song]= selected_song_uri
    print(st.session_state["input_songs"])

def callbackfun2():
    print("function 2 called....")
    st.session_state["retain_state"] = False



def callbackfun3():
    del st.session_state["input_songs"]
    st.session_state.df_key += 1


def get_full_detail_from_uris(uri_list):
    url = HOSTNAME + "uri"

    # Prepare the request body
    uri_params = {
        "uri_list": uri_list
    }

    # Make the get request
    response = requests.post(url, json=uri_params)

    if response.status_code == 200:
        songs = response.json()
        # Split each string in the list and create a list of dictionaries
        data = [dict(zip(['track_name', 'artist_name', 'album_name', 'track_uri'], item.split(', '))) for item in songs]
        # Create the DataFrame
        df = pd.DataFrame(data)
        return df
    else:
        print(f"get_full_detail_from_uris failed with response {response.status_code}")
        return None


def recommend():
    recommendation_container.header("Your recommendations:")
    uris = get_song_uri_from_pills()
    url = HOSTNAME + "get_recommendations"

    # Prepare the request body
    uri_params = {
        "uri_list": uris
    }

    print(uri_params)

    # Make the get request
    response = requests.post(url, json=uri_params)

    if response.status_code == 200:
        print("Recommendation API call is success!")
        content = response.json()
        print(content)
        recommendation_uri = content["recommendations"]
        recs = get_full_detail_from_uris(recommendation_uri)
        print(recs)
        recommendation_container.dataframe(recs, use_container_width=True)

def searchcache(track_name=None, artist_name=None, album_name=None):
    df = st.session_state["complete_set"]
    print(df.columns)
    filtered_df = df[
        (df['track_name'].str.contains(search_name, case=False, na=False) | (search_name == "")) &
        (df['artist_name'].str.contains(search_artist, case=False, na=False) | (search_artist == "")) &
        (df['album_name'].str.contains(search_album, case=False, na=False) | (search_album == ""))
    ]

    return filtered_df

def searchfun(track_name=None, artist_name=None, album_name=None):
    first_search_df = searchcache(track_name, artist_name, album_name)
    if first_search_df.empty:
        url = HOSTNAME + "/tracksearch"

        # Prepare the request body
        search_params = {
            "track_name": track_name,
            "artist_name": artist_name,
            "album_name": album_name
        }

        # Make the get request
        response = requests.post(url, json=search_params)

        if response.status_code == 200:
            songs = response.json()
            # Split each string in the list and create a list of dictionaries
            data = [dict(zip(['track_name', 'artist_name', 'album_name', 'track_uri'], item.split(', '))) for item in songs]
            # Create the DataFrame
            df = pd.DataFrame(data)
            return df
        else:
            return None
    else:
        return first_search_df



def get_all_songs():
    url = HOSTNAME + "all"

    response = requests.get(url)
    if response.status_code == 200:
        all_songs = response.json()
    else:
        # return an empty dataframe
        return pd.DataFrame()

    # with open('../../all_songs_v2.json', 'r', encoding='utf-8-sig') as f:
    #     all_songs = json.load(f)

    # Split each string in the list and create a list of dictionaries
    data = [dict(zip(['track_name', 'artist_name', 'album_name', 'track_uri'], item.split(', '))) for item in
            all_songs]
    # Create the DataFrame
    df = pd.DataFrame(data)

    return df


def get_song_uri_from_pills():
    names = st.session_state["song_pills"]
    uris = [st.session_state["input_songs"][name] for name in names]
    return uris




# Define the songs list at the top
# songs = [
#     {"title": "Song 1", "artist": "Artist 1", "album": "Album 1"},
#     {"title": "Song 2", "artist": "Artist 2", "album": "Album 2"},
#     {"title": "Song 3", "artist": "Artist 3", "album": "Album 3"},
#     {"title": "Song 4", "artist": "Artist 4", "album": "Album 4"},
#     {"title": "Song 5", "artist": "Artist 5", "album": "Album 5"},
#     {"title": "Blinding Lights", "artist": "The Weeknd", "album": "After Hours"},
#     {"title": "Shape of You", "artist": "Ed Sheeran", "album": "÷"},
#     {"title": "Sweet Caroline", "artist": "Neil Diamond", "album": "Brother Love's Travelling Salvation Show"},
#     {"title": "Hey Ya", "artist": "Outkast", "album": "Speakerboxxx/The Love Below"},
#     {"title": "Bohemian Rhapsody", "artist": "Queen", "album": "A Night at the Opera"},
#     {"title": "Hollaback Girl", "artist": "Gwen Stefani", "album": "Love. Angel. Music. Baby."},
#     {"title": "Yellow", "artist": "Coldplay", "album": "Parachutes"},
#     {"title": "Don't Stop Believin'", "artist": "Journey", "album": "Escape"},
#     {"title": "Cruel Summer", "artist": "Taylor Swift", "album": "Lover"},
#     {"title": "Bad Guy", "artist": "Billie Eilish", "album": "When We All Fall Asleep, Where Do We Go?"}
# ]

if not st.session_state["retain_state"]:
    songs = get_all_songs()
    songs_df = pd.DataFrame(songs)


# Create columns for logo and title
logo_col, title_col = st.columns([1, 5])

with logo_col:
    st.text("")
    st.image("https://storage.googleapis.com/pr-newsroom-wp/1/2023/05/Spotify_Primary_Logo_RGB_Green.png", width=100)  # Adjust width as needed
with title_col:
    # Title
    st.title("Loopify")

    # Subtitle
    st.markdown("""
        <style>
        .subtitle {
            font-size: 1.2em;
            color: #808080;
            margin-bottom: 2em;
        }
        </style>
        <div class="subtitle">Find your vibe with loopify music recommendation.</div>
    """, unsafe_allow_html=True)

# Replace the single search bar with multiple search fields in a row
col1, col2, col3, col4 = st.columns([3, 2, 2, 3])
with col1:
    search_name = st.text_input("Track Name", key="search_name", on_change=callbackfun2)
with col2:
    search_artist = st.text_input("Artist", key="search_artist")
with col3:
    search_album = st.text_input("Album", key="search_album")
with col4:
    search_button = st.button("Search Songs", key='search_button')



# Filter logic when search button is pressed
# st.write(st.session_state["retain_state"])
if not(st.session_state["retain_state"]):
    print("1")
    if search_button and (search_name or search_artist or search_album):
        print("1.2")
        print(">" + search_name + "<")
        print(not(search_name))
        # searchfun(search_name)
        # filtered_df = songs_df[
        #     (songs_df['track_name'].str.contains(search_name, case=False, na=False) | (search_name == "")) &
        #     (songs_df['artist_name'].str.contains(search_artist, case=False, na=False) | (search_artist == "")) &
        #     (songs_df['album_name'].str.contains(search_album, case=False, na=False) | (search_album == ""))
        # ]
        filtered_df = searchfun(search_name, search_artist, search_album)
    else:
        print("1.3")
        filtered_df = songs_df

    if "complete_set" not in st.session_state:
        print("1.4")
        st.session_state["complete_set"] = filtered_df
else:
    print("2")
    filtered_df = st.session_state["filtered_df"]

# dataframe

starttime = time.time()
print("3")
st.dataframe(
    filtered_df,
    use_container_width=True,
    key=f"song_df_{st.session_state.df_key}",
    on_select=callbackfun,
    selection_mode='single-row'
)
endtime = time.time()
print("time:", endtime - starttime)

if "input_songs" in st.session_state:
    print("herere...")
    selected_song_container = st.container(height=300)
    starttime = time.time()
    with selected_song_container:
        list_of_songs = st.session_state["input_songs"].keys()
        selection = st.pills("Your songs", list_of_songs, selection_mode="multi", default=list_of_songs, key="song_pills")

    col1, col2 = st.columns(2, gap="medium")

    with col1:
        st.button("Generate Song Recommendations", on_click=recommend, use_container_width=True)
    with col2:
        st.button("Clear all", on_click=callbackfun3, use_container_width=True)

    recommendation_container = st.container()
    endtime = time.time()
    print("time:", endtime - starttime)
