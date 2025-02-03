# Databricks notebook source
# MAGIC %md
# MAGIC ### Library imports

# COMMAND ----------

from pyspark.sql.functions import explode, col
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.feature import BucketedRandomProjectionLSHModel
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import collect_list, struct
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

import mlflow.spark


# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Class & Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Track indexer class

# COMMAND ----------

class Spotify_id_generator_DF:
    def __init__(self):
        # Create empty DataFrame with schema
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        self.schema = StructType([
            StructField("track", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        self.track_ids_df = spark.createDataFrame([], self.schema)
        self.max_num = 0

    def is_new_id(self, id):
        return self.track_ids_df.filter(f"track = '{id}'").count() == 0

    def is_new_reverse_id(self, id):
        return self.track_ids_df.filter(f"id = '{id}'").count() == 0

    def _add_id(self, id):
        from pyspark.sql.functions import lit
        new_row = spark.createDataFrame([(id, self.max_num + 1)], ["track", "id"])
        self.track_ids_df = self.track_ids_df.union(new_row)
        self.max_num += 1

    def add_ids(self, df, inputCol):
        from pyspark.sql.functions import monotonically_increasing_id, row_number
        from pyspark.sql.window import Window

        # Get unique tracks
        unique_tracks = df.select(inputCol).distinct()

        # Filter out existing tracks
        existing_tracks = self.track_ids_df.select("track")
        new_tracks = unique_tracks.join(existing_tracks, unique_tracks[inputCol] == existing_tracks.track, "left_anti")

        if new_tracks.count() > 0:
            # Create sequential IDs
            w = Window.orderBy(inputCol)
            new_pairs = new_tracks.withColumn(
                "id",
                row_number().over(w) + self.max_num
            )

            # Update track_ids_df
            self.track_ids_df = self.track_ids_df.union(
                new_pairs.select(col(inputCol).alias("track"), "id")
            )

            # Update max_num
            self.max_num = self.track_ids_df.agg({"id": "max"}).collect()[0][0]

    def get_id(self, id):
        if self.is_new_id(id):
            self._add_id(id)
        return self.track_ids_df.filter(f"track = '{id}'").first()["id"]

    def get_reverse_id(self, id):
        if not self.is_new_reverse_id(id):
            return self.track_ids_df.filter(f"id = '{id}'").first()["track"]
        return None

    def save(self, filepath):
        self.track_ids_df.write.parquet(filepath)

    def load(self, filepath):
        self.track_ids_df = spark.read.schema(self.schema).parquet(filepath)
        self.max_num = self.track_ids_df.agg({"id": "max"}).collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC #### JSON to Dataframe function

# COMMAND ----------

def convert_JSON_to_ALS_format(path, id_generator=None):
    # read the data
    spotify_json_df = spark.read.option("multiline", "true").json(path)
    playlists_df = spotify_json_df.select(explode("playlists").alias("playlist"))

    # Extract playlist_id and tracks
    playlist_tracks_df = playlists_df.select(
        col("playlist.pid").alias("playlist_id"),
        explode("playlist.tracks").alias("track")
    ).select("playlist_id", "track.track_uri", "track.track_name", "track.artist_name", "track.album_name")

    # Initialize or use existing id generator
    if id_generator is None:
        id_generator = Spotify_id_generator_DF()
        # create index (This line below makes things a little more faster by skipping a lot a union operations)
        id_generator.add_ids(df=playlist_tracks_df, inputCol="track_uri")
    else:
        # if using an existing id_generator then add any new tracks that is not indexed yet
        new_tracks = playlist_tracks_df.select("track_uri").subtract(id_generator.track_ids_df.select("track"))
        id_generator.add_ids(df=new_tracks, inputCol="track_uri")

    # join the index table with playlist_tracks_df
    playlist_track_indexed = playlist_tracks_df.join(id_generator.track_ids_df,
                                                     playlist_tracks_df["track_uri"] == id_generator.track_ids_df[
                                                         "track"],
                                                     "left").select(playlist_tracks_df["*"],
                                                                    # keep all original columns
                                                                    id_generator.track_ids_df["id"].alias(
                                                                        "track_uri_index")  # rename id column
                                                                    )

    # # Create UDF for id generation
    # get_track_id = udf(lambda x: id_generator.get_id(x), IntegerType())

    # # Apply id generation
    # playlist_track_indexed = playlist_tracks_df.withColumn(
    #     "track_uri_index",
    #     get_track_id("track_uri")
    # )

    # Add rating column
    playlist_track_rating_df = playlist_track_indexed.withColumn("rating", lit(1))

    # Remove duplicates
    playlist_track_rating_df = playlist_track_rating_df.distinct()

    return playlist_track_rating_df, id_generator


# COMMAND ----------

def convert_Tuple_to_ALS_format(track_list, user_item_df, id_generator):
    # get the "track.track_uri", "track.track_name", "track.artist_name", "track.album_name" from tracks
    new_user_item_df = user_item_df.select(["track_name", "artist_name", "album_name", "track_uri"]).distinct().filter(
        f"track_uri in {track_list}")
    new_user_item_df = new_user_item_df.withColumn("playlist_id", lit(111111))

    new_user_item_df.show()

    # join the index table with playlist_tracks_df
    playlist_track_indexed = new_user_item_df.join(id_generator.track_ids_df,
                                                   new_user_item_df["track_uri"] == id_generator.track_ids_df["track"],
                                                   "left").select(new_user_item_df["*"],  # keep all original columns
                                                                  id_generator.track_ids_df["id"].alias(
                                                                      "track_uri_index")  # rename id column
                                                                  )

    # Add rating column
    playlist_track_rating_df = playlist_track_indexed.withColumn("rating", lit(1))

    # Remove duplicates
    playlist_track_rating_df = playlist_track_rating_df.distinct()

    return playlist_track_rating_df


# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting Recommendations Function

# COMMAND ----------

def get_recommendations(user_id, lsh_model, rec_model, existing_users_matrix, new_users_matrix):
    similar_user = find_the_most_similar_user(LSH_model=lsh_model, existing_users_matrix=existing_users_matrix,
                                              key=get_key_from_user_matrix(user_id, user_item_matrix=new_users_matrix))
    recs = rec_model.recommendForUserSubset(similar_user.select("playlist_id"), NUMBER_OF_RECOMMENDATION)
    return recs, similar_user


# COMMAND ----------

# MAGIC %md
# MAGIC #### Sparse vector creation function

# COMMAND ----------

def create_sparse_user_vectors(df, max_index, userCol="playlist_id", itemCol="track_uri_index"):
    # Group by user and collect indices and values
    vector_df = df.groupBy(userCol).agg(
        F.sort_array(F.collect_list(F.col(itemCol).cast("int"))).alias("indices"),
        F.collect_list(F.lit(1.0)).alias("values")  # All values will be 1.0
    )

    # Create sparse vector UDF
    def to_sparse_vector(indices, values):
        return Vectors.sparse(max_index + 1, indices, values)

    sparse_vector_udf = F.udf(to_sparse_vector, VectorUDT())

    # Create sparse vectors
    return vector_df.select(
        F.col(userCol),
        sparse_vector_udf("indices", "values").alias("features")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC #### ANN functions for finding similar users

# COMMAND ----------

def find_the_most_similar_user(LSH_model, existing_users_matrix, key):
    # Find approximate nearest neighbors
    k = 1  # number of nearest neighbors to find
    similar_users = LSH_model.approxNearestNeighbors(dataset=existing_users_matrix,
                                                     key=key,
                                                     numNearestNeighbors=k
                                                     ).select("playlist_id", "distCol")
    return similar_users


def get_key_from_user_matrix(user_id, user_item_matrix):
    return user_item_matrix.where(f"playlist_id={user_id}").select(["features"]).first()["features"]


# COMMAND ----------

# MAGIC %md
# MAGIC #### Function to decode the indexed tracks

# COMMAND ----------

def decode_track(rec_df, user_item_df):
    # Join with original dataset to get song names
    final_df = rec_df.join(
        user_item_df,
        user_item_df["track_uri_index"] == rec_df["index"],
        "left"
    ).select("index", "track_name", "artist_name", "track_uri").distinct()

    return final_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

# input file
NEW_USER_PATH = None
TUPLE_OF_SONGS = tuple(dbutils.widgets.get("uri_list").split(","))

# models
# REC_PATH = "/mnt/models/rec_model_v6"
# LSH_PATH = "/mnt/models/lsh_model_v6"
REC_PATH = "runs:/7ef176e2b7da434f96efab603ac7fc82/recommender"
LSH_PATH = "runs:/44ea3a6839cd45b8b1aa2c931d86688c/lsh"
IDX_PATH = "/mnt/models/idx_model_v6"

# existing user data
EXISTING_USERS_MATRIX = "/mnt/pythonobjectdata/existing_users_matrix_v6.parquet"
EXISTING_USERS_DF = "/mnt/pythonobjectdata/user_item_df_v6.parquet"

# recommendation engine params
NUMBER_OF_RECOMMENDATION = 10
VECTOR_MAX_INDEX = 2262292

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load models

# COMMAND ----------

# track id indexer
id_generator = Spotify_id_generator_DF()
indexer_fit = id_generator.load(IDX_PATH)

# recommendation model
rec_model = mlflow.spark.load_model(REC_PATH).stages[0]

# LSH model
lsh_model = mlflow.spark.load_model(LSH_PATH).stages[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load existing user data

# COMMAND ----------

existing_users_matrix = spark.read.format("parquet").load(EXISTING_USERS_MATRIX)

# COMMAND ----------

user_item_df = spark.read.format("parquet").load(EXISTING_USERS_DF)

# COMMAND ----------

# remove this snippet
# user_item_df.write.parquet('/dbfs/FileStore/requirements.txt')
# indexer_fit.save(IDX_PATH)
# rec_model.save("/dbfs/FileStore/rec_model")
# existing_users_matrix.write.parquet("/dbfs/FileStore/existing_users_matrix.parquet")
# lsh_model.save("/dbfs/FileStore/lsh_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert New User Data to Recommendation Engine Format

# COMMAND ----------

# Note: Imporve this part after implementing the API
# create the new user matrix
if NEW_USER_PATH:
    new_users_df, _ = convert_JSON_to_ALS_format(NEW_USER_PATH, id_generator=indexer_fit)
else:
    print("creating news users df...")
    new_users_df = convert_Tuple_to_ALS_format(track_list=TUPLE_OF_SONGS, user_item_df=user_item_df,
                                               id_generator=id_generator)

new_users_matrix = create_sparse_user_vectors(max_index=VECTOR_MAX_INDEX, df=new_users_df)

# use_saved_model = True
new_users_list = new_users_df.select("playlist_id").distinct().rdd.map(lambda x: x["playlist_id"]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Recommendations

# COMMAND ----------

# loop through users
for user in new_users_list:
    # time for recommendations
    user_id = user

    print(f"Recommending {NUMBER_OF_RECOMMENDATION} tracks for new playlist id {user_id}")

    # then simply recommend for the most similar user
    recs, similar_user = get_recommendations(user_id, lsh_model, rec_model, existing_users_matrix, new_users_matrix)
    print(">> Most similar playlist for this new one is..")
    similar_user.show()

    # decode recs to show the song name
    recs_decoded = decode_track(
        recs.select(explode("recommendations").alias("recs")).select(col("recs.track_uri_index").alias("index")),
        user_item_df)
    recs_decoded.show()

# COMMAND ----------

# Prepare the output you want to return
output = {
    "recommendations": recs_decoded.select("track_uri").rdd.map(lambda row: row.track_uri).collect(),
    "input_tracks": tuple(dbutils.widgets.get("uri_list").split(","))
}

# Convert the output to JSON and return it
import json

dbutils.notebook.exit(json.dumps(output, indent=4))