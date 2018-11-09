## Paralell Java Kafka consumer and producer

### Execution
folow first 2 steps https://kafka.apache.org/quickstart

but in step 3:

```bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic sensor
```
then the java kafka consumer and producer are working

### Build
 buld using  the producer with ```./gradlew build``` for the consumer please use ```gradle shadow```
execute via idea or ```java - jar```.
