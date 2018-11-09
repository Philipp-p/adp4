package kafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Producer implements Runnable{
    int id;
    int numValues;
    String topic;
    int partition;
    Properties props;

    public Producer(int id, int numValues, String topic, int partition, Properties props) {
        this.id = id;
        this.numValues = numValues;
        this.topic = topic;
        this.partition = partition;
        this.props = props;
    }

    public static void main(String[] args) throws InterruptedException {

        final int numValues = 100;
        final String topic = "sensor";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer p1 = new Producer(0, numValues / 2, topic, 0, props);
        Producer p2 = new Producer(1, numValues / 2, topic, 1, props);

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(p1);
        executor.submit(p2);

        executor.shutdown();

        try {
            if (!executor.awaitTermination(100000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

    }

    private static void produceMessages(int id, int numValues, String topic, int partition, Properties props) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < numValues; i++) {
            ProducerRecord<String, String> data;

            Gson gson = new GsonBuilder().create();

            String tmp = gson.toJson(new Message(id));

            data = new ProducerRecord<String, String>(topic, partition, id + ": " + Integer.toString(i),tmp );
            producer.send(data);
            Thread.sleep(1L);
        }
        producer.close();
    }


    @Override
    public void run() {
        try {
            produceMessages(id, numValues, topic, partition, props);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

