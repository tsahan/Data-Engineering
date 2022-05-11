import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"]=config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Creates a new Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song data from S3 and creates song and artist dimensional tables.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    songSchema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("title", Str()),
        Fld("year", Int())
    ])

    df = spark.read.json(song_data, schema=songSchema)
    # extract columns to create songs table
    songs_table = df.select("title", "artist_id", "year", "duration").dropDuplicates() \
                    .withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing song table to S3")
    songs_table.write.parquet(output_data + "songs", partitionBy=["year", "artist_id"], mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select([
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("lattitude"),
        col("artist_longitude").alias("longitude")
    ]).dropDuplicates()
    
    # write artists table to parquet files
    print("Writing artist table to S3")
    artists_table.write.parquet(output_data + "artists", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Loads log data from S3 and creates user and time dimensional tables.
    Extracts columns from joined song and log datasets to create songplays table.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select([
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level"
    ]).dropDuplicates()
    
    # write users table to parquet files
    print("Writing users table to S3")
    users_table.write.parquet(output_data + "users", partitionBy=["level"], mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time")) \
                   .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday") \
                   .drop_duplicates() 
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table to S3")
    time_table.write.parquet(output_data + "time", partitionBy=["year","month"], mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song==song_df.title) & (df.artist == song_df.artist_name), "left")   \
                        .withColumn("songplay_id", monotonically_increasing_id())
    
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, "inner") \
                                     .select("songplay_id", songplays_table.start_time,
                                             col("userId").alias("user_id"),
                                             "level",
                                             "song_id",
                                             "artist_id",
                                             col("sessionId").alias("session_id"),
                                             "location",
                                             col("userAgent").alias("user_agent"),
                                             "year", 
                                             "month").drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays table to S3")
    songplays_table.write.parquet(output_data + "songplays", partitionBy=["year","month"], mode="overwrite")


def main():
    """
    Initialize ETL pipeline to load data from S3, process the data into analytics tables using Spark, and load them back into S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


main()
