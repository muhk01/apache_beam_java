# Apache Beam In Dataflow with Java API
Contents : 

BQ - to - RDBMS

BQ - to NoSQL Cassandra

BQ - to GCS

## Maven compile Dataflow Runner

mvn compile exec:java \
    -Dexec.mainClass=$CLASS \
    -Dexec.args="--project=project-name-1234 \
    --tempLocation=gs://bucket_name/tmp \
    --runner=DataflowRunner \
    --usePublicIps=false \
    --region=$REGION \
    --subnetwork=regions/$REGION/subnetworks/$SUBNET \
    --numWorkers=1 \
    --maxNumWorkers=1 \
    --env=${ENVIRONMENT}"
