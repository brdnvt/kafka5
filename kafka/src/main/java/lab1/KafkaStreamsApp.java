package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsApp {
    private static final String INPUT_TOPIC = "coffee_products";
    private static final String NO_MILK_TOPIC = "no_milk_drinks";
    private static final String COCONUT_MILK_TOPIC = "coconut_milk_drinks";
    private static final String OTHER_MILK_TOPIC = "other_milk_drinks";
    private static final String HIGH_CALORIE_COUNT_TOPIC = "high_calorie_count";
    private static final String NO_MILK_CALORIES_SUM_TOPIC = "no_milk_calories_sum";
    private static final String WINDOWED_CALORIES_TOPIC = "windowed_calories";
    private static final String WINDOWED_COUNT_TOPIC = "windowed_count";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L); 
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); 
        props.put(StreamsConfig.STATE_DIR_CONFIG, "kafka-streams-state");

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> highCalorieDrinks = inputStream.filter((key, value) -> {
            try {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("calories").getAsInt() > 200;
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
                return false;
            }
        });

        highCalorieDrinks
            .groupBy((key, value) -> "total")
            .count(Materialized.as("high-calorie-count-store"))
            .toStream()
            .mapValues(Object::toString)
            .to(HIGH_CALORIE_COUNT_TOPIC);

        highCalorieDrinks.split()
            .branch((key, value) -> {
                try {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return json.get("milk").getAsInt() == 0;
                } catch (Exception e) {
                    return false;
                }
            }, Branched.withConsumer(ks -> ks.to(NO_MILK_TOPIC)))
            .branch((key, value) -> {
                try {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return json.get("milk").getAsInt() == 5;
                } catch (Exception e) {
                    return false;
                }
            }, Branched.withConsumer(ks -> ks.to(COCONUT_MILK_TOPIC)))
            .branch((key, value) -> {
                try {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    int milk = json.get("milk").getAsInt();
                    return milk != 0 && milk != 5;
                } catch (Exception e) {
                    return false;
                }
            }, Branched.withConsumer(ks -> ks.to(OTHER_MILK_TOPIC)));

        inputStream
            .filter((key, value) -> {
                try {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return json.get("milk").getAsInt() == 0;
                } catch (Exception e) {
                    return false;
                }
            })
            .groupBy((key, value) -> "total")
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> {
                    try {
                        JsonObject json = gson.fromJson(value, JsonObject.class);
                        return aggregate + json.get("calories").getAsInt();
                    } catch (Exception e) {
                        return aggregate;
                    }
                },
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("no-milk-calories-sum-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer())
            )
            .toStream()
            .mapValues(Object::toString)
            .to(NO_MILK_CALORIES_SUM_TOPIC);

        Duration windowSize = Duration.ofMinutes(5);
        Duration retentionPeriod = Duration.ofHours(24);
        
        inputStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> {
                    try {
                        JsonObject json = gson.fromJson(value, JsonObject.class);
                        return aggregate + json.get("calories").getAsInt();
                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                        return aggregate;
                    }
                },
                Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("windowed-calories-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer())
                    .withRetention(retentionPeriod)
            )
            .toStream()
            .map((key, value) -> KeyValue.pair(
                String.format("%s@%d", key.key(), key.window().startTime().toEpochMilli()),
                value != null ? value.toString() : "0"
            ))
            .to(WINDOWED_CALORIES_TOPIC);

        Duration advanceBy = Duration.ofMinutes(1);
        inputStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceBy))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("windowed-count-store")
                .withRetention(retentionPeriod))
            .toStream()
            .map((key, value) -> KeyValue.pair(
                String.format("%s@%d", key.key(), key.window().startTime().toEpochMilli()),
                value != null ? value.toString() : "0"
            ))
            .to(WINDOWED_COUNT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("Caught unhandled Streams exception: " + exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        File stateDir = new File(props.getProperty(StreamsConfig.STATE_DIR_CONFIG));
        if (stateDir.exists()) {
            for (File file : stateDir.listFiles()) {
                file.delete();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Streams application gracefully...");
            streams.close(Duration.ofSeconds(5));
        }));

        streams.start();
    }
}
