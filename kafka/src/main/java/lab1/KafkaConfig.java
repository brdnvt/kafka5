package lab1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.opencsv.CSVReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaConfig {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "coffee-products";
    private static final String CSV_FILE_PATH = "starbucks.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
            
            reader.readNext();
            
            String[] line;
            while ((line = reader.readNext()) != null) {
                String json = String.format(
                    "{\"product_name\":\"%s\",\"size\":\"%s\",\"milk\":%s,\"whip\":%s," +
                    "\"serv_size_m_l\":%s,\"calories\":%s,\"total_fat_g\":%s,\"saturated_fat_g\":%s," +
                    "\"trans_fat_g\":%s,\"cholesterol_mg\":%s,\"sodium_mg\":%s,\"total_carbs_g\":%s," +
                    "\"fiber_g\":%s,\"sugar_g\":%s,\"caffeine_mg\":%s}",
                    line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7],
                    line[8], line[9], line[10], line[11], line[12], line[13], line[14]
                );

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, json);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Повідомлення відправлено успішно: " + json);
                    } else {
                        System.err.println("Помилка при відправці повідомлення: " + exception.getMessage());
                    }
                });
            }
            
            System.out.println("Всі дані успішно відправлено в Kafka");
            
        } catch (Exception e) {
            System.err.println("Помилка при роботі з Kafka: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 