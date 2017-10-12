# DocAudit
Data pipeline for auditing large document datasets.    
The general concept can be applied to any type of document but the scenario that this project currently covers is email auditing.  

# Dependencies  
* Python 2.7  
  * elasticsearch Python module  
* Pyspark 
* Elasticsearch  
* Kibana (optional)  
* AWS (optional)  

# Email Auditing Scenario
A test email corpus was generated for this scenario using the novel, "War and Peace".  
A copy of the corpus can be found at https://archive.org/download/warandpeace02600gut/wrnpc11.txt  
"generate_emails_pyspark.py" generates emails from the source.  
"index_emails_pyspark.py" indexes the generated emails in the Elasticsearch server.  
Querying can be done through the Elasticsearch REST API, the elasticsearch Python module, or the Elastic Kibana webgui.    

Elasticsearch GET Request: curl -XGET localhost:9200/index/type/id

# Pipeline  
1. Spark  
  1.1 generate_emails_pyspark.py  
  1.2 index_emails_pyspark.py  
2. Elasticsearch  
3. Kibana 

# Demo  
https://youtu.be/HlVocr7P7Ik
