package lab1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Properties;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class KafkaStreamsApp {    private static final String INPUT_TOPIC = "coffee_products";
    private static final String NO_MILK_TOPIC = "no_milk_drinks";
    private static final String COCONUT_MILK_TOPIC = "coconut_milk_drinks";
    private static final String OTHER_MILK_TOPIC = "other_milk_drinks";
    private static final String HIGH_CALORIE_COUNT_TOPIC = "high_calorie_count";
    private static final String NO_MILK_CALORIES_SUM_TOPIC = "no_milk_calories_sum";
    
    // Топіки для моніторингу виробника з різними типами вікон
    private static final String PRODUCER_METRICS_TUMBLING_TOPIC = "producer_metrics_tumbling";
    private static final String PRODUCER_METRICS_HOPPING_TOPIC = "producer_metrics_hopping";
    private static final String PRODUCER_METRICS_SESSION_TOPIC = "producer_metrics_session";    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-app-v5"); // ЗМІНЕНО ID ДЛЯ ОЧИСТКИ
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
          // Налаштування для правильної агрегації
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // Збільшуємо інтервал
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // 10MB кеш для агрегації
        
        // ДОДАЄМО ОЧИСТКУ STATE ПРИ КОЖНОМУ ЗАПУСКУ
        props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 0);
        
        // Очищуємо state directory перед запуском
        cleanStateDirectory();        // Створюємо топіки перед запуском стрімів
        createTopicsIfNotExist("localhost:9092");
        
        // ОЧИЩУЄМО РЕЗУЛЬТАТИ ПЕРЕД ЗАПУСКОМ
        cleanResultTopics("localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);
        
        // Створюємо окремі потоки для різних типів молока (без фільтрації за калоріями)
        Map<String, KStream<String, String>> allMilkBranches = inputStream
            .split(Named.as("all-milk-type-"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 0;
            }, Branched.as("no-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 5;
            }, Branched.as("coconut-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                int milk = json.get("milk").getAsInt();
                return milk != 0 && milk != 5;
            }, Branched.as("other-milk"))
            .noDefaultBranch();

        // Фільтруємо висококалорійні напої з основного потоку
        KStream<String, String> highCalorieStream = inputStream
            .filter((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("calories").getAsInt() > 200;
            });

        // Розділяємо висококалорійні напої за типом молока
        Map<String, KStream<String, String>> highCalorieBranches = highCalorieStream
            .split(Named.as("high-calorie-milk-type-"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 0;
            }, Branched.as("no-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                return json.get("milk").getAsInt() == 5;
            }, Branched.as("coconut-milk"))
            .branch((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                int milk = json.get("milk").getAsInt();
                return milk != 0 && milk != 5;
            }, Branched.as("other-milk"))
            .noDefaultBranch();
        
        // Виводимо висококалорійні напої в відповідні топіки
        highCalorieBranches.get("high-calorie-milk-type-no-milk").to(NO_MILK_TOPIC);
        highCalorieBranches.get("high-calorie-milk-type-coconut-milk").to(COCONUT_MILK_TOPIC);
        highCalorieBranches.get("high-calorie-milk-type-other-milk").to(OTHER_MILK_TOPIC);

        // Агрегатор 1: Підрахунок кількості висококалорійних напоїв
        highCalorieStream
            .groupBy((key, value) -> "total")
            .count(Materialized.as("high-calorie-count-store"))
            .toStream()
            .mapValues(Object::toString)
            .to(HIGH_CALORIE_COUNT_TOPIC);

        // Агрегатор 2: Сума калорій у ВСІХ напоях без молока (не тільки висококалорійних)
        allMilkBranches.get("all-milk-type-no-milk")
            .groupBy((key, value) -> "total")
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return aggregate + json.get("calories").getAsInt();
                },
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("no-milk-calories-sum-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Integer())
            )            .toStream()
            .mapValues(Object::toString)
            .to(NO_MILK_CALORIES_SUM_TOPIC);

        // ========= ОПЕРАЦІЇ ЗІ ЗБЕРЕЖЕННЯМ СТАНУ З РІЗНИМИ ТИПАМИ ВІКОН =========
        // ========= МОНІТОРИНГ ВИРОБНИКА (варіант 14 mod 3 = 2) =========
        
        // 1. TUMBLING WINDOW (30 секунд) - неперетинні вікна
        // Підрахунок кількості повідомлень у фіксованих вікнах по 30 секунд
        inputStream
            .groupBy((key, value) -> "producer_throughput")
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("producer-tumbling-count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()))
            .toStream()
            .map((windowedKey, count) -> {
                String key = String.format("tumbling_%d_%d", 
                    windowedKey.window().start(), 
                    windowedKey.window().end());
                JsonObject metrics = new JsonObject();
                metrics.addProperty("window_type", "tumbling");
                metrics.addProperty("window_start", windowedKey.window().start());
                metrics.addProperty("window_end", windowedKey.window().end());
                metrics.addProperty("message_count", count);
                metrics.addProperty("window_duration_ms", 30000);
                metrics.addProperty("producer_id", "starbucks-producer");
                return KeyValue.pair(key, metrics.toString());
            })
            .to(PRODUCER_METRICS_TUMBLING_TOPIC);

        // 2. HOPPING WINDOW (30 секунд з кроком 10 секунд) - перетинні вікна  
        // Моніторинг середньої кількості калорій у ковзних вікнах
        inputStream
            .groupBy((key, value) -> "producer_calories_monitoring")
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30), Duration.ofSeconds(5))
                .advanceBy(Duration.ofSeconds(10)))
            .aggregate(
                () -> new double[]{0.0, 0.0}, // [сума_калорій, кількість_повідомлень]
                (key, value, aggregate) -> {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    int calories = json.get("calories").getAsInt();
                    aggregate[0] += calories; // сума калорій
                    aggregate[1] += 1.0;     // кількість повідомлень
                    return aggregate;
                },
                Materialized.<String, double[], WindowStore<Bytes, byte[]>>as("producer-hopping-calories-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.serdeFrom(
                        (topic, data) -> gson.toJson(data).getBytes(),
                        (topic, data) -> gson.fromJson(new String(data), double[].class)
                    ))
            )
            .toStream()
            .map((windowedKey, caloriesData) -> {
                String key = String.format("hopping_%d_%d", 
                    windowedKey.window().start(), 
                    windowedKey.window().end());
                double avgCalories = caloriesData[1] > 0 ? caloriesData[0] / caloriesData[1] : 0.0;
                JsonObject metrics = new JsonObject();
                metrics.addProperty("window_type", "hopping");
                metrics.addProperty("window_start", windowedKey.window().start());
                metrics.addProperty("window_end", windowedKey.window().end());
                metrics.addProperty("total_calories", caloriesData[0]);
                metrics.addProperty("message_count", (int)caloriesData[1]);
                metrics.addProperty("avg_calories_per_drink", Math.round(avgCalories * 100.0) / 100.0);
                metrics.addProperty("window_duration_ms", 30000);
                metrics.addProperty("advance_ms", 10000);
                metrics.addProperty("producer_id", "starbucks-producer");
                return KeyValue.pair(key, metrics.toString());
            })
            .to(PRODUCER_METRICS_HOPPING_TOPIC);        // 3. SESSION WINDOW (5 секунд неактивності) - динамічні вікна
        // Моніторинг сесій активності виробника за типом молока
        inputStream
            .map((key, value) -> {
                JsonObject json = gson.fromJson(value, JsonObject.class);
                String milkType;
                int milk = json.get("milk").getAsInt();
                if (milk == 0) milkType = "no_milk";
                else if (milk == 5) milkType = "coconut_milk";
                else milkType = "other_milk";
                return KeyValue.pair(milkType, value);
            })
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5)))            .aggregate(
                () -> new int[]{0, 0, 0}, // [кількість, мін_калорії, макс_калорії]
                (key, value, aggregate) -> {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    int calories = json.get("calories").getAsInt();
                    aggregate[0] += 1; // кількість
                    if (aggregate[0] == 1) {
                        aggregate[1] = calories; // мін калорії
                        aggregate[2] = calories; // макс калорії
                    } else {
                        aggregate[1] = Math.min(aggregate[1], calories); // мін калорії
                        aggregate[2] = Math.max(aggregate[2], calories); // макс калорії
                    }
                    return aggregate;
                },
                (aggKey, leftAgg, rightAgg) -> {
                    // Merger для об'єднання сесій
                    int[] result = new int[3];
                    result[0] = leftAgg[0] + rightAgg[0]; // загальна кількість
                    result[1] = Math.min(leftAgg[1], rightAgg[1]); // мінімальні калорії
                    result[2] = Math.max(leftAgg[2], rightAgg[2]); // максимальні калорії
                    return result;
                },
                Materialized.<String, int[], SessionStore<Bytes, byte[]>>as("producer-session-milk-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.serdeFrom(
                        (topic, data) -> gson.toJson(data).getBytes(),
                        (topic, data) -> gson.fromJson(new String(data), int[].class)
                    ))
            )
            .toStream()
            .map((windowedKey, sessionData) -> {
                String key = String.format("session_%s_%d_%d", 
                    windowedKey.key(),
                    windowedKey.window().start(), 
                    windowedKey.window().end());
                long sessionDuration = windowedKey.window().end() - windowedKey.window().start();
                JsonObject metrics = new JsonObject();
                metrics.addProperty("window_type", "session");
                metrics.addProperty("milk_type", windowedKey.key());
                metrics.addProperty("session_start", windowedKey.window().start());
                metrics.addProperty("session_end", windowedKey.window().end());
                metrics.addProperty("session_duration_ms", sessionDuration);
                metrics.addProperty("message_count", sessionData[0]);
                metrics.addProperty("min_calories", sessionData[1]);
                metrics.addProperty("max_calories", sessionData[2]);
                metrics.addProperty("inactivity_gap_ms", 5000);
                metrics.addProperty("producer_id", "starbucks-producer");
                return KeyValue.pair(key, metrics.toString());
            })
            .to(PRODUCER_METRICS_SESSION_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
    private static void cleanStateDirectory() {
        try {
            String stateDir = System.getProperty("java.io.tmpdir") + "\\kafka-streams\\starbucks-streams-app-v5";
            java.io.File dir = new java.io.File(stateDir);
            if (dir.exists()) {
                deleteDirectory(dir);
                System.out.println("Очищено state directory: " + stateDir);
            }
        } catch (Exception e) {
            System.err.println("Помилка при очищенні state directory: " + e.getMessage());
        }
    }
    
    private static void deleteDirectory(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
    
    private static void createTopicsIfNotExist(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
          try (AdminClient adminClient = AdminClient.create(adminProps)) {
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(
                new NewTopic(INPUT_TOPIC, 1, (short) 1),
                new NewTopic(NO_MILK_TOPIC, 1, (short) 1),
                new NewTopic(COCONUT_MILK_TOPIC, 1, (short) 1),
                new NewTopic(OTHER_MILK_TOPIC, 1, (short) 1),
                new NewTopic(HIGH_CALORIE_COUNT_TOPIC, 1, (short) 1),
                new NewTopic(NO_MILK_CALORIES_SUM_TOPIC, 1, (short) 1),
                // Топіки для моніторингу виробника з різними типами вікон
                new NewTopic(PRODUCER_METRICS_TUMBLING_TOPIC, 1, (short) 1),
                new NewTopic(PRODUCER_METRICS_HOPPING_TOPIC, 1, (short) 1),
                new NewTopic(PRODUCER_METRICS_SESSION_TOPIC, 1, (short) 1)
            ));
            
            // Очікуємо завершення створення топіків
            result.all().get();
            System.out.println("Топіки створено або вже існують");
        } catch (ExecutionException e) {
            if (e.getCause().getMessage().contains("already exists")) {
                System.out.println("Топіки вже існують");
            } else {
                System.err.println("Помилка при створенні топіків: " + e.getMessage());
            }        } catch (InterruptedException e) {
            System.err.println("Перервано при створенні топіків: " + e.getMessage());
        }
    }
    
    private static void cleanResultTopics(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Видаляємо топіки результатів
            adminClient.deleteTopics(Arrays.asList(
                HIGH_CALORIE_COUNT_TOPIC,
                NO_MILK_CALORIES_SUM_TOPIC,
                PRODUCER_METRICS_TUMBLING_TOPIC,
                PRODUCER_METRICS_HOPPING_TOPIC,
                PRODUCER_METRICS_SESSION_TOPIC
            )).all().get();
            
            System.out.println("Очищено топіки результатів");
            
            // Чекаємо трохи перед повторним створенням
            Thread.sleep(2000);
            
        } catch (Exception e) {
            System.out.println("Топіки результатів не існували або вже очищені: " + e.getMessage());
        }
    }
}
