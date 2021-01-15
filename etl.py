import configparser
from datetime import datetime
import os
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

    """
    Description: This function will connect to the given amazon S3 bucket and traverse the song_data directory and load all       the contained json files into a spark dataframe. This song and the artist data will be extracted and then separate           parquet files will be created and loaded into S3 

    Arguments:
        spark: the spark context. 
        input_data: the amazon S3 bucket that contains the song data.
        output_data: the amazon S3 bucket used to store the output.

    Returns:
        None
    """
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "/song-data/*/*/*/*.json"
    
    # read song data file
    df_song = spark.read.json(input_data + song_data)
    
    # extract columns to create songs table
    songs_table = df_song.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "song", mode="overwrite")

    # extract columns to create artists table
    artist_table = df_song.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude' , 'artist_longitude')
    artist_table = artist_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artist_table.write.parquet(output_data + "artist", mode="overwrite")

    """
    Description: This function will connect to the given amazon S3 bucket and traverse the log-data directory and load all       the contained json files into a spark dataframe. The user, time and songplays parquet tables will be created from the         resultant dataframe. Both the songplays and time parquet files will be organised by year and month in that respective         order.

    Arguments:
        spark: the spark context. 
        input_data: the amazon S3 bucket that contains the log data.
        output_data: the amazon S3 bucket used to store the output.

    Returns:
        None
    """
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "log-data/*/*/*.json"

    # read log data file
    df_log = spark.read.json(input_data + log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    user_table = df_log.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level')
    user_table = user_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "user", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.select( "timestamp",
                                hour("timestamp").alias("hour"),
                                dayofmonth("timestamp").alias("day"),
                                weekofyear("timestamp").alias("week"),    
                                month("timestamp").alias("month"),
                                dayofweek("timestamp").alias("weekday"),    
                                year("timestamp").alias("year")) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_data", mode="overwrite") 
    time_table = time_table.dropDuplicates(['timestamp'])
    
    df_song.createOrReplaceTempView("songs")
    df_log.createOrReplaceTempView("events")

    songplays_table = spark.sql("""SELECT distinct timestamp,
    userId as user_id, level, song_id, artist_id, sessionId as session_id, location, userAgent as user_agent 
    from events, songs 
    where events.artist = songs.artist_name
    and events.song = songs.title
    and events.length = songs.duration""")    
    
    songplays_table = songplays_table.withColumn("year", year(col("timestamp")))
    songplays_table = songplays_table.withColumn("month", month(col("timestamp")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_path + "songplays", mode="overwrite")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityrichardf/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
