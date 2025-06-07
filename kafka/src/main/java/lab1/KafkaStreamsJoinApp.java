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
    private static final String NUTRITION_TOPIC = "nutrition-info";
    private static final String HIGH_CALORIE_NUTRITION_TOPIC = "high-calorie-nutrition";
    private static final String LOW_CALORIE_NUTRITION_TOPIC = "low-calorie-nutrition";
    private static final String JOINED_NUTRITION_TOPIC = "joined-nutrition-info";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "nutrition-streams-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> nutritionStream = builder.stream(NUTRITION_TOPIC);

        KStream<String, String> highCalorieStream = nutritionStream.filter((key, value) -> {
            try {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.has("calories") && json.get("calories").getAsInt() >= 200;
            } catch (Exception e) {
                return false;
            }
        });

        KStream<String, String> lowCalorieStream = nutritionStream.filter((key, value) -> {
            try {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.has("calories") && json.get("calories").getAsInt() < 200;
            } catch (Exception e) {
                return false;
            }
        });

        highCalorieStream.to(HIGH_CALORIE_NUTRITION_TOPIC);
        lowCalorieStream.to(LOW_CALORIE_NUTRITION_TOPIC);

        KStream<String, String> joinedStream = highCalorieStream.join(
            lowCalorieStream,
            (highCalorie, lowCalorie) -> {
                try {
                    JsonObject high = gson.fromJson(highCalorie, JsonObject.class);
                    JsonObject low = gson.fromJson(lowCalorie, JsonObject.class);
                    
                    JsonObject result = new JsonObject();
                    result.addProperty("high_calorie_product", high.get("product_name").getAsString());
                    result.addProperty("high_calorie_value", high.get("calories").getAsInt());
                    result.addProperty("low_calorie_product", low.get("product_name").getAsString());
                    result.addProperty("low_calorie_value", low.get("calories").getAsInt());
                    
                    return result.toString();
                } catch (Exception e) {
                    return null;
                }
            },
            JoinWindows.of(Duration.ofMinutes(5)),
            StreamJoined.with(
                Serdes.String(),
                Serdes.String(),
                Serdes.String()
            )
        );

        joinedStream.to(JOINED_NUTRITION_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
