# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting Azure Blob Storage

# COMMAND ----------

# Check if the mount point exists
def check_mount(mount_point):
    mounted_points = [mount.mountPoint for mount in dbutils.fs.mounts()]
    return "/mnt/" + mount_point in mounted_points


# COMMAND ----------

mount_locations = ["dataset", "models", "pythonobjectdata"]
for loc in mount_locations:
    if not check_mount(loc):
        dbutils.fs.mount(source=f"wasbs://{loc}@spotifyrecsys.blob.core.windows.net",
                         mount_point="/mnt/" + loc,
                         extra_configs={"fs.azure.account.key.spotifyrecsys.blob.core.windows.net": dbutils.secrets.get(
                             scope="BLOB_KEY", key="spotifyrecsysStoragekey1")})
        print(f"Notebook mounted to {loc}.")
    else:
        print(f"Notebook already mounted to {loc}.")

# COMMAND ----------

# # USE THIS SNIPPET TO RESOLVE ANY MOUNTING ISSUE. (UNMOUNTING AND REMOUNTING HELPS SOLVES THE ISSUE)
# # To unmount multiple mount points in a loop
# mount_points = ["/mnt/mpddata", "/mnt/dataset", "/mnt/models", "/mnt/pythonobjectdata"]
# for mount_point in mount_points:
#     try:
#         dbutils.fs.unmount(mount_point)
#         print(f"Successfully unmounted: {mount_point}")
#     except Exception as e:
#         print(f"Error unmounting {mount_point}: {str(e)}")

# COMMAND ----------

# configurations
use_saved_model = True
use_saved_data = True
PATH = "/mnt/dataset/mpd.slice.*.json"
MULTILINE = True
NEW_USER_PATH = None
REC_PATH = "/mnt/models/rec_model_v6"
LSH_PATH = "/mnt/models/lsh_model_v6"
IDX_PATH = "/mnt/models/idx_model_v6"
EXISTING_USERS_MATRIX = "/mnt/pythonobjectdata/existing_users_matrix_v6.parquet"
EXISTING_USERS_DF = "/mnt/pythonobjectdata/user_item_df_v6.parquet"
NUMBER_OF_RECOMMENDATION = 5
VECTOR_MAX_INDEX = 2262292

# COMMAND ----------

from pyspark.sql.functions import explode, col
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import collect_list, struct
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType


# load the data and limit it to small amount to test things out quickly


# initialize spark if not using databricks


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


def convert_JSON_to_ALS_format(path, id_generator=None, multifile=False):
    """
    Parameters:
    path (str): Path to a single JSON file or a regex like path to multiple json files. For eg. "/mnt/mpddata/mpd.slice.*.json"
    id_generator (Spotify_id_generator_DF, optional): An instance of the Spotify_id_generator_DF class. If None, a new instance will be created. Default is None.
    multifile (bool, optional): If True, reads multiple files from the directory specified in the path. Default is False.
    """
    # read the data
    if not multifile:
        spotify_json_df = spark.read.option("multiline", "true").json(path)
    else:
        spotify_json_df = spark.read.option("multiline", "true").option("recursiveFileLookup", "true").json(path)

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


def create_recommendation_engine(user_item_df):
    als = ALS(maxIter=5, regParam=0.01, userCol="playlist_id", itemCol="track_uri_index", ratingCol="rating",
              coldStartStrategy="drop", implicitPrefs=True, alpha=10)

    # use ALS to model the recommendation
    model = als.fit(user_item_df)

    return model


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


def create_user_vectors(df, reference_df=None, userCol="playlist_id", itemCol="track_uri_index"):
    # If reference_df is provided, use its items as reference
    if reference_df is not None:
        # Get all unique items from reference dataset
        all_items = sorted([int(c) for c in reference_df.columns if c != userCol and c != "features"])
    else:
        # Get all unique items from current dataset
        all_items = sorted(df.select(itemCol).distinct().rdd.map(lambda x: int(x[itemCol])).collect())

    # Pivot the data with all possible items
    pivot_df = df.groupBy(userCol).pivot(itemCol, all_items).agg(
        F.coalesce(F.first("rating"), F.lit(0))
    ).fillna(0)

    # Ensure all item columns exist and are in correct order
    for item in all_items:
        if str(item) not in pivot_df.columns:
            pivot_df = pivot_df.withColumn(str(item), F.lit(0))

    # Get feature columns in correct order
    feature_cols = [str(item) for item in all_items]

    # Assemble features into vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    return assembler.transform(pivot_df)


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


def is_old_user(user_id, reference_df):
    return user_item_df.filter(f"playlist_id={user_id}").first() is not None


# COMMAND ----------

def decode_track(rec_df, user_item_df):
    # Join with original dataset to get song names
    final_df = rec_df.join(
        user_item_df,
        user_item_df["track_uri_index"] == rec_df["index"],
        "left"
    ).select("index", "track_name", "artist_name").distinct()

    return final_df


# COMMAND ----------

# create existing user dataframe and convert it to a format appropriate for ALS
if use_saved_data:
    print("re-using EXISTING_USERS_DF and indexer model...")
    # load user item df
    user_item_df = spark.read.parquet(EXISTING_USERS_DF)
    # load id generator
    id_generator = Spotify_id_generator_DF()
    id_generator.load(IDX_PATH)
    indexer_fit = id_generator
else:
    print("creating a new USERS_DF and indexer model...")
    user_item_df, indexer_fit = convert_JSON_to_ALS_format(path=PATH, multifile=MULTILINE)
    user_item_df.write.parquet(EXISTING_USERS_DF)
    indexer_fit.save(IDX_PATH)

# COMMAND ----------

# duplicate track_uri_index test (no row should appear)
display(user_item_df.groupBy("playlist_id", "track_uri_index").count().filter("count > 2"))

# COMMAND ----------

# create recommendation engine
if use_saved_model:
    print("re-using model...")
    from pyspark.ml.recommendation import ALSModel

    rec_model = ALSModel.load(REC_PATH)
else:
    print("creating a new model...")
    rec_model = create_recommendation_engine(user_item_df)
    # save recommendation model
    rec_model.save(REC_PATH)

# COMMAND ----------

# use Locality Sensitive Hashing to cluster similar users playlist vectors in the same bucket
# https://spark.apache.org/docs/latest/ml-features.html#locality-sensitive-hashing
# use_saved_model = False
if use_saved_data:
    # Explicit format specification
    print("re-using existing_users_matrix...")
    existing_users_matrix = spark.read.format("parquet").load(EXISTING_USERS_MATRIX)
else:
    print("creating a new existing_users_matrix...")
    existing_users_matrix = create_sparse_user_vectors(max_index=VECTOR_MAX_INDEX, df=user_item_df)
    existing_users_matrix.write.parquet(EXISTING_USERS_MATRIX)
# use_saved_model = True

# COMMAND ----------


# use_saved_model = False
if use_saved_model:
    print("using saved model...")
    from pyspark.ml.feature import BucketedRandomProjectionLSHModel

    # load the lsh model
    lsh_model = BucketedRandomProjectionLSHModel.load(LSH_PATH)
else:
    print("creating a new model for lsh...")
    # Initialize and fit LSH model
    lsh = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol="hashes",
        numHashTables=3,
        bucketLength=0.999799458143871
    )

    lsh_model = lsh.fit(existing_users_matrix)
    # save lsh model
    lsh_model.save(LSH_PATH)
# use_saved_model = True

# COMMAND ----------

# create the new user matrix
new_users_df, _ = convert_JSON_to_ALS_format(NEW_USER_PATH, id_generator=indexer_fit)
# use_saved_model = False
if use_saved_model:
    new_users_matrix = spark.read.format("parquet").load("dbfs:/FileStore/new_users_matrix.parquet")
else:
    new_users_matrix = create_sparse_user_vectors(max_index=VECTOR_MAX_INDEX, df=new_users_df)
# use_saved_model = True
new_users_list = new_users_df.select("playlist_id").distinct().rdd.map(lambda x: x["playlist_id"]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test new user item dataframe for duplicate track ids within each user

# COMMAND ----------

# duplicate track_uri_index test (no row should appear)
display(new_users_df.groupBy("playlist_id", "track_uri_index").count().filter("count > 2"))


# COMMAND ----------

def get_recommendations(user_id, lsh_model, existing_users_matrix, new_users_matrix):
    similar_user = find_the_most_similar_user(LSH_model=lsh_model, existing_users_matrix=existing_users_matrix,
                                              key=get_key_from_user_matrix(user_id, user_item_matrix=new_users_matrix))
    recs = rec_model.recommendForUserSubset(similar_user.select("playlist_id"), NUMBER_OF_RECOMMENDATION)
    return recs, similar_user


# COMMAND ----------


# loop through users
for user in new_users_list:
    # time for recommendations
    user_id = user
    if is_old_user(user_id, user_item_df):
        print(f"Recommending {NUMBER_OF_RECOMMENDATION} tracks for existing playlist id {user_id}")
        # simply recommend tracks
        recs = rec_model.recommendForUserSubset(spark.createDataFrame([(user_id,)], ["playlist_id"]),
                                                NUMBER_OF_RECOMMENDATION)
    else:
        print(f"Recommending {NUMBER_OF_RECOMMENDATION} tracks for new playlist id {user_id}")
        # then simply recommend for that user
        recs, similar_user = get_recommendations(user_id, lsh_model, existing_users_matrix, new_users_matrix)
        print(">> Most similar playlist for this new one is..")
        similar_user.show()

    # decode recs to show the song name
    recs_decoded = decode_track(
        recs.select(explode("recommendations").alias("recs")).select(col("recs.track_uri_index").alias("index")),
        user_item_df)
    recs_decoded.show()