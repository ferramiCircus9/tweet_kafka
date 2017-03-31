# tweet_kafka

## Para Conteo en shell
  **cat nuevo2.txt | grep [3-9] | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test**
  
## Ingreso en un archivo
  **/opt/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic streams-wordcount-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter  --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer >> nuevo2.txt**
  
## Conteo kafka
   **/opt/kafka/bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo**

## Ingreso al topic especial 
   ** cat /tmp/file-input.txt | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input**

## Pasos

* 1
	** python **

* 2
	** /opt/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181           --topic streams-wordcount-output           --from-beginning           --formatter kafka.tools.DefaultMessageFormatter           --property print.key=true           --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer           --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer > twe_pre.txt **

* 3
	** cat twe_pre.txt | grep "[3-9]$" | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test **

* 4 
	** /opt/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning **
