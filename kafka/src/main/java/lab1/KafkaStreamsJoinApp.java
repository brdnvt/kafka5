package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsJoinApp {
    private static final String DRINKS_TOPIC = "drinks-info";
    private static final String NUTRITION_TOPIC = "nutrition-info";
    private static final String HIGH_CALORIE_TOPIC = "high-calorie-drinks";
    private static final String LOW_CALORIE_TOPIC = "low-calorie-drinks";
    private static final String JOINED_TOPIC = "complete-drinks-info";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "drinks-streams-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> drinksStream = builder.stream(DRINKS_TOPIC);
        KStream<String, String> nutritionStream = builder.stream(NUTRITION_TOPIC);

        KStream<String, String> highCalorieDrinks = drinksStream.filter((key, value) -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            return json.get("calories").getAsInt() >= 200;
        });

        KStream<String, String> lowCalorieDrinks = drinksStream.filter((key, value) -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            return json.get("calories").getAsInt() < 200;
        });

        highCalorieDrinks.to(HIGH_CALORIE_TOPIC);
        lowCalorieDrinks.to(LOW_CALORIE_TOPIC);

        KStream<String, String> joinedStream = drinksStream.join(
            nutritionStream,
            (drinkInfo, nutritionInfo) -> {
                JsonObject drink = gson.fromJson(drinkInfo, JsonObject.class);
                JsonObject nutrition = gson.fromJson(nutritionInfo, JsonObject.class);
                
                for (String key : nutrition.keySet()) {
                    if (!key.equals("product_name")) {  
                        drink.add(key, nutrition.get(key));
                    }
                }
                return drink.toString();
            },
            JoinWindows.of(Duration.ofMinutes(5)),  
            StreamJoined.with(
                Serdes.String(),
                Serdes.String(), 
                Serdes.String()
            )
        );

        joinedStream.to(JOINED_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
