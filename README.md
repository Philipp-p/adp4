## Paralell Java Kafka consumer and producer

### Execution
folow first 2 steps https://kafka.apache.org/quickstart

but in step 3:

```bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic sensor
```
then the java kafka consumer and producer are working

### Build
 buld using  the consumer and producer with ```./gradlew build``` each
execute via idea or ```java - jar```, ther is a possible bug with gradle build in the jar of the consumer since it does exhibit strange behavior. The jar does simply terminate on my pc without output, executed in idea it is perfectly fine
