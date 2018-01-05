# POC1: Kafka -> Spark -> Cassandra data flow
Demonstrates analytics on Streaming data simulated via a Kafka topic.
Sample data used is [Airline Ontime](https://www.transtats.bts.gov/Tables.asp?DB_ID=120&DB_Name=Airline%20On-Time%20Performance%20Data&DB_Short_Name=On-Time) data.


### Prerequisites
1. Kafka running on localhost with a topic named `testspark`
2. Cassandra running on localhost with a keyspace named `cloudpoc`

### Building
`mvn package -DskipTests=true`

### Running
`{project-dir}$ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0  --class org.pgmx.cloud.poc.poc1.KafkaReaderTest target/poc1-1.0.jar`