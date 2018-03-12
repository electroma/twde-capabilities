# start
sudo zookeeper-server-start /etc/kafka/zookeeper.properties
sudo kafka-server-start /etc/kafka/server.properties
python3 kafka-python-producer/kafka_producer.py

spark-submit --jars /twde-capabilities/config-1.3.2.jar --class com.thoughtworks.ca.de.streaming.KafkaConsumer --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 /twde-capabilities/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar

