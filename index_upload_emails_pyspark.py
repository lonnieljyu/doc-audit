'''
index_Upload_Email_By_Partitions_pyspark.py

Usage: spark-submit index_upload_emails_pyspark.py <hdfs server> <Elasticsearch hosts file>
'''

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import json
from elasticsearch import Elasticsearch, helpers

# Set configuration strings
hdfs_server_address = "hdfs://" + sys.argv[1] + ":9000"
hdfs_working_directory = "/war_and_peace_old/"
input_directory = hdfs_server_address + hdfs_working_directory

with open(sys.argv[2], 'r') as hosts_file:
    es_server_addresses = [address.strip() for address in hosts_file]

number_of_chunks = 1 #100

# Set Elasticsearch bulk upload options
index = 'war_and_peace'
chunk_size = 500
max_chunk_bytes = 104857600
thread_count = 6
queue_size = 6

def Get_Indexed_Email_Dict(email_json):
    email_dict = json.loads(email_json)
    email_dict['_index'] = index
    email_dict['_type'] = email_dict['From']
    email_dict['_id'] = email_dict['Id']
    return email_dict

def Upload_Email_By_Partition(partition_index, es_server_addresses, email_jsons):
    if partition_index > 0: return
    
    es = Elasticsearch(es_server_addresses, sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
    email_dicts = [Get_Indexed_Email_Dict(email_json) for email_json in email_jsons]
    helpers.bulk(es, email_dicts)
    # helpers.parallel_bulk(es, email_dicts, thread_count, chunk_size, max_chunk_bytes, queue_size)
    
### Main

# Set up spark configuration
conf = SparkConf().setAppName("Index and Upload Emails")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

for chunk_index in range(number_of_chunks):
    rdd = sc.textFile(input_directory + "emails" + str(chunk_index))
    # rdd = rdd.map(lambda email_json: Get_Indexed_Email_Dict(email_json))
    # rdd.foreachPartition(lambda email_jsons: Upload_Email_By_Partition(es_server_addresses, email_jsons))
    rdd.mapPartitionsWithIndex(lambda partition_index, email_jsons: Upload_Email_By_Partition(partition_index, es_server_addresses, email_jsons))
