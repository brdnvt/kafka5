package lab1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaStreamResultsConsumer {
    private static final String[] TOPICS = {
        "no_milk_drinks",
        "coconut_milk_drinks",
        "other_milk_drinks",
        "high_calorie_count",
        "no_milk_calories_sum",
        "windowed_calories",
        "windowed_count"
    };

    private static final DateTimeFormatter formatter = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-results-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Add reliability configurations
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");

        final AtomicBoolean stopConsumer = new AtomicBoolean(false);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumer...");
            stopConsumer.set(true);
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Arrays.asList(TOPICS));

            while (!stopConsumer.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    String output;
                    
                    if (topic.equals("windowed_calories") || topic.equals("windowed_count")) {
                        String[] parts = record.key().split("@");
                        String key = parts[0];
                        String timestamp = formatter.format(
                            Instant.ofEpochMilli(Long.parseLong(parts[1]))
                        );
                        
                        output = String.format("Віконна операція - Тема: %s%n" +
                                            "  Ключ: %s%n" +
                                            "  Час вікна: %s%n" +
                                            "  Значення: %s%n",
                            topic,
                            key,
                            timestamp,
                            record.value());
                    } else {
                        output = String.format("%s - Ключ: %s, Значення: %s%n",
                            topic,
                            record.key(),
                            record.value());
                    }
                    System.out.print(output);
                }
                
                // Manual commit
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        System.err.println("Commit failed for offsets: " + exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            // Ignore if closing
            if (!stopConsumer.get()) throw e;
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync(); // Final commit
            } finally {
                consumer.close();
                System.out.println("Consumer closed");
            }
        }
    }
}
