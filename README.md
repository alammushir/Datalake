Sparkify Datalake
-----------------
This Project is to process song and logs datasets into a Star Schema datalake.

The dataset files reside on an S3 folder from where it has to be read and then processed.
The results shall then be written back into another S3 folder.
The solution will use Spark(Pyspark) to process the files

Input files
-----------------
Songs: contains metadata about the songs like title, artist, duration
Logs: User activity logs from the sparkify application


Datalake schema:
-----------------
Dimension tables
-----------------
songs
users
artists
time

Fact tables
-----------------
songplays
