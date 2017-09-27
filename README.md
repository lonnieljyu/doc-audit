# DocAudit
Data pipeline for auditing large document datasets.    
The general concept can be applied to any type of document but the scenario that this project currently covers is email auditing.  

# Dependencies  
Python 2.7  
Pyspark 
Elasticsearch  
Kibana  
AWS (optional)  

# Email Auditing Scenario
A test email corpus was generated for this scenario using the novel, "War and Peace".  
A copy of the corpus can be found at https://archive.org/download/warandpeace02600gut/wrnpc11.txt  
"generate_emails_pyspark.py" generates emails from the source.  
"index_upload_emails_pyspark.py" indexes the generated emails in the Elasticsearch server.

# Pipeline  
Spark  
Elasticsearch  
Kibana 

# Demo  
<url coming soon>  
  
