"""
index_emails_pyspark.py

Usage: spark-submit index_emails_pyspark.py <hdfs server> 
        <Elasticsearch hosts file> <ES username> <ES password>
"""

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import json
from elasticsearch import Elasticsearch, helpers

# Set global variables
NUMBER_OF_CHUNKS = 100
INDEX = "war_and_peace_test"
TYPE = "emails"

# Set directory strings
HDFS_SERVER_ADDRESS = "hdfs://" + sys.argv[1] + ":9000"
HDFS_WORKING_DIRECCTORY = "/war_and_peace_time/"
INPUT_DIRECTORY = HDFS_SERVER_ADDRESS + HDFS_WORKING_DIRECCTORY

def get_indexed_email_dict(email_json):
    email_dict = json.loads(email_json)
    email_dict["_index"] = INDEX
    email_dict["_type"] = TYPE
    email_dict["_id"] = email_dict["Absolute Id"]
    email_dict["date"] = email_dict["Datetime"]
    del email_dict["Datetime"]
    return email_dict

def bulk_index_emails_by_partition(es_server_addresses, es_username, 
        es_password, email_jsons):    
    es = Elasticsearch(es_server_addresses, http_auth=(es_username, es_password), 
        sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
    email_dicts = [get_indexed_email_dict(email_json) for email_json in email_jsons]
    helpers.bulk(es, email_dicts)

if __name__ == "__main__":
    
    # Get Elasticsearch hosts addresses
    with open(sys.argv[2], "r") as hosts_file:
        es_server_addresses = [address.strip() for address in hosts_file]
        
    # Get ES credentials
    es_username = sys.argv[3]
    es_password = sys.argv[4]
    
    # Set up spark configuration
    conf = SparkConf().setAppName("Index Emails")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    for chunk_index in range(NUMBER_OF_CHUNKS):
        input_working_directory = INPUT_DIRECTORY + "emails" + str(chunk_index)
        rdd = sc.textFile(input_working_directory)
        rdd.foreachPartition(lambda email_jsons: 
            bulk_index_emails_by_partition(es_server_addresses, es_username, es_password, 
                email_jsons))
        