"""
generate_emails_pyspark.py

Usage: spark-submit generate_emails_pyspark.py <hdfs server>
"""

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from email.mime.text import MIMEText
from datetime import datetime
import random
import json

# Set configuration strings
HDFS_SERVER_ADDRESS = "hdfs://" + str(sys.argv[1]) + ":9000"
INPUT_FILE_PATH = HDFS_SERVER_ADDRESS + "/wrnpc11.txt"
HDFS_WORKING_DIRECTORY = "/war_and_peace_test/"

# Set number of books and chunks
# One book is 16 MB
# One chunk of books is 5 GB = 5120 MB = 1024 MB * 5 = 16 MB * 64 * 5 = 16 MB * 2^6 * 5
# All chunks is 500 GB = 5 GB * 100
TIMES_DOUBLE_RDD = 6
NUMBER_OF_DOUBLED_RDD = 5
NUMBER_OF_BOOKS_IN_CHUNK = 320
NUMBER_OF_CHUNKS = 100
START_CHUNK_INDEX = 0
END_CHUNK_INDEX = START_CHUNK_INDEX + NUMBER_OF_CHUNKS

def process_email_into_json(id, message):
    """ Process (id, line) tuple into email json """
    
    character_username_list = ["Kirill_Bezukhov", "Pierre_Bezukhov", 
    "Nikolai_Bolkonsky", "Andrei_Bolkonsky", "Maria_Bolkonskaya", "Ilya_Rostov", 
    "Natasha_Rostova", "Nikolenka_Rostov", "Sonya_Rostova", "Petya_Rostov", 
    "Vasily_Kuragin", "Helene_Kuragina", "Anatole_Kuragin", "Ippolit_Kuragin", 
    "Boris_Drubetskoy", "Anna_Drubetskaya", "Fyodor_Dolokhov", "Adolf_Berg", 
    "Anna_Scherer", "Maria_Akhrosimova", "Amalia_Bourienne", "Vasily Dmitrich_Denisov", 
    "Platon_Karataev", "Osip_Bazdeyev", "Mikhail_Kutuzov", "Napoleon_Bonaparte"]
    
    # Randomly choose sender and receiver
    sender = random.choice(character_username_list)
    receiver = random.choice(character_username_list)
    while(receiver == sender):
        receiver = random.choice(character_username_list)
    
    # Construct MIME email headers and body
    message = MIMEText(message)
    message["Subject"] = "War and Peace - " + str(id)
    message["From"] = sender + "@warandpeace.com"
    message["To"] = receiver + "@warandpeace.com"
    email = dict(message)
    email["Body"] = message.get_payload()
    email["Datetime"] = str(datetime.utcnow())
    email["Absolute Id"] = id
    return json.dumps(email)
    
if __name__ == "__main__":
    
    # Set up spark configuration
    conf = SparkConf().setAppName("Generate Emails")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    # Ingest data into RDD and clean data
    book_rdd = sc.textFile(INPUT_FILE_PATH)
    book_rdd = book_rdd.filter(lambda line: line is not None and len(line.strip()) > 0)
    number_of_lines_in_chunk = NUMBER_OF_BOOKS_IN_CHUNK * book_rdd.count()
    
    # Double the number of book copies 
    books_rdd = book_rdd
    for i in range(TIMES_DOUBLE_RDD):
        books_rdd = books_rdd.union(books_rdd)
        
    # Copy the doubled rdd
    chunk_rdd = sc.emptyRDD()
    for i in range(NUMBER_OF_DOUBLED_RDD):
        chunk_rdd = chunk_rdd.union(books_rdd)
    books_rdd.unpersist()
    chunk_rdd = chunk_rdd.zipWithIndex()
    
    for chunk_index in range(START_CHUNK_INDEX, END_CHUNK_INDEX):
    
        # Remap (message, relative id) to (email json str)
        chunk_start_index = chunk_index * number_of_lines_in_chunk
        indexed_chunk_rdd = chunk_rdd.map(lambda (message, relative_id): 
            process_email_into_json(id = relative_id + chunk_start_index, 
                message = message.strip()) )
        hdfs_output_directory = HDFS_WORKING_DIRECTORY + "emails" + str(chunk_index) + "/"
        indexed_chunk_rdd.saveAsTextFile(HDFS_SERVER_ADDRESS + hdfs_output_directory)
        