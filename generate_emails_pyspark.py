'''
generate_emails_pyspark.py

Usage: spark-submit generate_emails_pyspark.py <hdfs server>
'''

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from email.mime.text import MIMEText
from datetime import datetime
import random
import json

# Set configuration strings
hdfs_server_address = "hdfs://" + str(sys.argv[1]) + ":9000"
input_file_path = hdfs_server_address + "/wrnpc11.txt"
hdfs_working_directory = "/war_and_peace/"

# Set number of books and chunks
# One book is 16 MB
# One collection of books is 5 GB = 5120 MB = 1024 MB * 5 = 16 MB * 64 * 5 = 16 MB * 2^6 * 5
# The whole collection is 500 GB = 5 GB * 100
times_double_rdd = 6
number_of_doubled_rdd = 5
number_of_books_in_chunk = 320
number_of_chunks = 100
start_chunk_index = 0
end_chunk_index = start_chunk_index + number_of_chunks

# Process (id, line) tuple into email json
def Process_Email_Into_Json(id, message):
    character_username_list = ["Kirill_Bezukhov", "Pierre_Bezukhov", "Nikolai_Bolkonsky", "Andrei_Bolkonsky", "Maria_Bolkonskaya", "Ilya_Rostov", "Natasha_Rostova", "Nikolenka_Rostov", "Sonya_Rostova", "Petya_Rostov", "Vasily_Kuragin", "Helene_Kuragina", "Anatole_Kuragin", "Ippolit_Kuragin", "Boris_Drubetskoy", "Anna_Drubetskaya", "Fyodor_Dolokhov", "Adolf_Berg", "Anna_Scherer", "Maria_Akhrosimova", "Amalia_Bourienne", "Vasily Dmitrich_Denisov", "Platon_Karataev", "Osip_Bazdeyev", "Mikhail_Kutuzov", "Napoleon_Bonaparte"]
    
    # Randomly choose sender and receiver
    sender = random.choice(character_username_list)
    receiver = random.choice(character_username_list)
    while(receiver == sender):
        receiver = random.choice(character_username_list)
    
    # Construct MIME email headers and body
    message = MIMEText(message)
    message['Subject'] = "War and Peace - " + str(id)
    message['From'] = sender + '@warandpeace.com'
    message['To'] = receiver + '@warandpeace.com'
    email = dict(message)
    email['Body'] = message.get_payload()
    email['Datetime'] = str(datetime.utcnow())
    email['Absolute Id'] = id
    return json.dumps(email)

### Main

# Set up spark configuration
conf = SparkConf().setAppName("Generate Emails")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Ingest data into RDD and clean data
book_rdd = sc.textFile(input_file_path)
# book_rdd = sc.textFile("wrnpc11.txt")

book_rdd = book_rdd.filter(lambda line: line is not None and len(line.strip()) > 0)
number_of_lines_in_book = book_rdd.count()
number_of_lines_in_chunk = number_of_books_in_chunk * number_of_lines_in_book

# Double the number of book copies 
books_rdd = book_rdd
for i in range(times_double_rdd):
    books_rdd = books_rdd.union(books_rdd)

# Copy the doubled rdd
chunk_rdd = sc.emptyRDD()
for i in range(number_of_doubled_rdd):
    chunk_rdd = chunk_rdd.union(books_rdd)
books_rdd.unpersist()
chunk_rdd = chunk_rdd.zipWithIndex()

for chunk_index in range(start_chunk_index, end_chunk_index):

    # Remap (message, relative id) to (email json str)
    chunk_start_index = chunk_index * number_of_lines_in_chunk
    indexed_chunk_rdd = chunk_rdd.map(lambda (message, relative_id): Process_Email_Into_Json(id = relative_id + chunk_start_index, message = message.strip()))

    hdfs_output_directory = hdfs_working_directory + "emails" + str(chunk_index) + "/"
    indexed_chunk_rdd.saveAsTextFile(hdfs_server_address + hdfs_output_directory)
    