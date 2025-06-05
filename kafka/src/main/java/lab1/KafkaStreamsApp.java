package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Branched;

import java.util.Properties;

public class KafkaStreamsApp {
    private static final String INPUT_TOPIC = "coffee_products";
    private static final String NO_MILK_TOPIC = "no_milk_drinks";
    private static final String COCONUT_MILK_TOPIC = "coconut_milk_drinks";
    private static final String OTHER_MILK_TOPIC = "other_milk_drinks";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> highCalorieDrinks = inputStream.filter((key, value) -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            return json.get("calories").getAsInt() > 200;
        });

        highCalorieDrinks.split()
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 0;
            }, Branched.withConsumer(ks -> ks.to(NO_MILK_TOPIC)))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 5;
            }, Branched.withConsumer(ks -> ks.to(COCONUT_MILK_TOPIC)))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                int milk = json.get("milk").getAsInt();
                return milk != 0 && milk != 5;
            }, Branched.withConsumer(ks -> ks.to(OTHER_MILK_TOPIC)));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
