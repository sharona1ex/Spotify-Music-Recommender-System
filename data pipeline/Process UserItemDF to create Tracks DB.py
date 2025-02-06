# Databricks notebook source
EXISTING_USERS_DF = "/mnt/pythonobjectdata/user_item_df_v6.parquet"
TRACK_DF = "/mnt/pythonobjectdata/track_df.parquet"

# COMMAND ----------

user_item_df = spark.read.parquet(EXISTING_USERS_DF)

# COMMAND ----------

user_item_df.columns

# COMMAND ----------

track_df = user_item_df.select(["track_name", "artist_name", "album_name", "track_uri"]).distinct()

# COMMAND ----------

track_df = track_df.withColumn("artist_name", track_df["artist_name"].substr(1, 200))
track_df = track_df.withColumn("track_name", track_df["track_name"].substr(1, 200))
track_df = track_df.withColumn("album_name", track_df["album_name"].substr(1, 200))


# COMMAND ----------

track_df.write.mode("overwrite").parquet(TRACK_DF)

# COMMAND ----------

