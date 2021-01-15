## Project purpose

The purpose of this database is to be able to analyse how Sparkify is being used by it's users. It has been decided to transform the data and create parquet files in S3 as doing analysis on raw json text files is not the easiest. Additionally,  the size of the data will be increasing exponentially as more users join and play songs then it has been decided to use this storage format due to it's column orientated storage design, meaning it will be particularly good at queries that involve a lot of column aggregations.

## Schema description

We have a fact table called Songplays and dimension tables named users, artists, songs and time. Songplays has been used as a fact table as the main entity/event the company is interested in is the actual streaming of a song (a songplay). This will make it very quick to execute queries on song streams and only require joins if additional information such as artist/song name are needed.

## Example queries

### Check the most played songs in order

select song_id, count(song_id) from songplays group by song_id order by count(song_id) desc

### Check the most played artists
select artist_id, count(artist_id) from songplays group by artist_id order by count(artist_id) desc

## Running the code

1. Fill in the correct values for the AWS_ACCESS_KEY_ID and the AWS_SECRET_ACCESS_KEY in the dl.cfg file

2. Run "etl.py" script. This will connect to the given amazon S3 bucket and traverse the song_data directory and load all the    contained json files into a spark dataframe. This song and the artist data will be extracted and then separate parquet        files will be created and loaded into S3. The songs' parquet files will be organised in directories, first by year, then      secondly by artist. This will then be repeated for the log json files. The user, time and songplays parquet tables will be    created from the resultant dataframe. Both the songplays and time parquet files will be organised by year and month in        that respective order.