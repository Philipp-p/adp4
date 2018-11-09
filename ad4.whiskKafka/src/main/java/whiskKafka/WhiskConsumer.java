package whiskKafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class WhiskConsumer {

    public static JsonObject main(JsonObject args) {
        String name = "stranger";
        if (args.has("name"))
            name = args.getAsJsonPrimitive("name").getAsString();
        JsonObject response = new JsonObject();
        response.addProperty("greeting", "Hello " + name + "!");

        System.out.println("Booting up consumers");
        String topic = "sensor";
        int numOfPolls = 5;
        int id = 0;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        Gson gson = new GsonBuilder().create();
        List<Message> messageList = new ArrayList<>();

        for (int i = 0; i < numOfPolls; ++i){
            ConsumerRecords<String, String> recs = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            if (recs.count() != 0) {
                for (ConsumerRecord<String, String> rec : recs) {
                    //System.out.println("Consumer: " + id + " Recieved " + rec.key() + " " + rec.value());
                    messageList.add(gson.fromJson(rec.value(), Message.class));
                }
            }
        }


        JsonElement jsonElement = gson.toJsonTree(messageList);
        JsonObject jsonObject = (JsonObject) jsonElement;
        return jsonObject;
    }

}
