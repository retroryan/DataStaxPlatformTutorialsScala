Docs:
https://docs.datastax.com/en/datastax_enterprise/4.8/datastax_enterprise/spark/sparkIntro.html


dse spark-jobserver start
dse spark-jobserver stop

view web UI:
http://52.88.210.79:8090/

###  Job Server Basic Read Write Demo - JobServerBasicReadWrite.scala

create schema:
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE IF NOT EXISTS test.people (id INT, name TEXT, birth_date TIMESTAMP, PRIMARY KEY (id));

build jar file:
sbt package


upload jar:
curl --data-binary @target/scala-2.10/sparkjobserver_2.10-0.1.0.jar 52.88.210.79:8090/jars/JobServerDemo

verify jar file was uploaded. with either UI or curl
   In the UI go under the jars tab.
   curl 52.88.210.79:8090/jars

curl -d "" '52.88.210.79:8090/contexts/sample-context'
curl -d "action=create" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=demo.JobServerBasicReadWrite'
curl -d "action=load" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=demo.JobServerBasicReadWrite&context=sample-context'
curl -d "action=display, person_id=3" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=demo.JobServerBasicReadWrite&context=sample-context'

curl -d "action=test_sql" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=demo.JobServerBasicReadWrite&context=sample-context'
curl -d "action=test_hc" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=demo.JobServerBasicReadWrite&context=sample-context'



###  DSE Spark Job Server Demo Commands - DemoSparkJob.scala

curl -d "action = create, keyspace = test, table = demo" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=com.datastax.DemoSparkJob'
curl -d "" '52.88.210.79:8090/contexts/test-context'
curl 52.88.210.79:8090/contexts
curl -d "action = cache, keyspace = test, table = demo, rdd = demoRDD" '52.88.210.79:8090/jobs?appName=JobServerDemo&classPath=com.datastax.DemoSparkJob&context=test-context'