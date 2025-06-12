package lab1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ProducerMetricsConsumer {
    private static final String[] TOPICS = {
        "producer_metrics"
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "producer-metrics-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS));

        System.out.println("=== Producer Metrics Consumer запущено ===");
        System.out.println("Очікування метрик з топіку producer_metrics...\n");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("=== PRODUCER METRICS ===\n");
                    System.out.printf("Топік: %s\n", record.topic());
                    System.out.printf("Ключ: %s\n", record.key());
                    System.out.printf("Значення: %s\n", record.value());
                    System.out.printf("Partition: %d, Offset: %d\n", 
                        record.partition(), record.offset());
                    System.out.println("========================\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
