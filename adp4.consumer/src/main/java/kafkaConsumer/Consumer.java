package kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer implements Runnable{
    int id;
    String topic;
    int numOfPolls;
    Properties props;

    public Consumer(int id, String topic, int numOfPolls, Properties props) {
        this.id = id;
        this.topic = topic;
        this.numOfPolls = numOfPolls;
        this.props = props;
    }

    public static void main(String[] args) {
        String topic = "sensor";
        int numOfPolls = 100;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

        Consumer c1 = new Consumer(0, topic, numOfPolls / 2 , props);
        Consumer c2 = new Consumer(1, topic, numOfPolls / 2 , props);

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(c1);
        executor.submit(c2);

        executor.shutdown();

        try {
            if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

    }

    private static void consume(int id ,String topic, int numOfPolls, Properties props) throws InterruptedException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));

        for (int i = 0; i < numOfPolls; ++i){
            ConsumerRecords<String, String> recs = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            if (recs.count() != 0) {
                for (ConsumerRecord<String, String> rec : recs) {
                    System.out.printf("Consumer: %d Recieved %s %s\n", id, rec.key(), rec.value());
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            consume(id, topic, numOfPolls, props);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

