'''
index_Upload_Email_By_Partitions_pyspark.py

Usage: spark-submit index_upload_emails_pyspark.py <hdfs server> <Elasticsearch hosts file> <ES username> <ES password>
'''

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import json
from elasticsearch import Elasticsearch, helpers

# Set global variables
number_of_chunks = 100
index = 'war_and_peace'
type = 'emails'

# Set directory strings
hdfs_server_address = "hdfs://" + sys.argv[1] + ":9000"
hdfs_working_directory = "/war_and_peace_time/"
input_directory = hdfs_server_address + hdfs_working_directory

# Get Elasticsearch hosts addresses
with open(sys.argv[2], 'r') as hosts_file:
    es_server_addresses = [address.strip() for address in hosts_file]
    
# Get ES credentials
es_username = sys.argv[3]
es_password = sys.argv[4]

def Get_Indexed_Email_Dict(email_json):
    email_dict = json.loads(email_json)
    email_dict['_index'] = index
    email_dict['_type'] = type
    email_dict['_id'] = email_dict['Absolute Id']
    email_dict['date'] = email_dict['Datetime']
    del email_dict['Datetime']
    return email_dict

def Upload_Email_By_Partition(es_server_addresses, es_username, es_password, email_jsons):    
    es = Elasticsearch(es_server_addresses, http_auth=(es_username, es_password), sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
    email_dicts = [Get_Indexed_Email_Dict(email_json) for email_json in email_jsons]
    helpers.bulk(es, email_dicts)
    
### Main

# Set up spark configuration
conf = SparkConf().setAppName("Index and Upload Emails")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

for chunk_index in range(number_of_chunks):
    input_working_directory = input_directory + "emails" + str(chunk_index)
    rdd = sc.textFile(input_working_directory)
    rdd.foreachPartition(lambda email_jsons: Upload_Email_By_Partition(es_server_addresses, es_username, es_password, email_jsons))
