# s3_bucket_search

Simple pair of scripts to search for directories with selected names in s3 bucket. Designed to handle milions of records

## Get Objects
First script, get_objects.py, is used to get all directories from s3 bucket. It uses boto3 library to connect to s3 and get all objects from selected bucket. It saves all objects to file in txt format.

## main.py
Second script, main.py, is used to search for directories with selected names. It reads all objects from file and search for directories with selected names. For now, searched name is hardcoded in script. It uses multiprocessing to speed up searching process. 
