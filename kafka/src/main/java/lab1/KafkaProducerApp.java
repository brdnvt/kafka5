package lab1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaProducerApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // ... existing code ...
        /*
        Random rand = new Random();
        for (int i = 0; i < 100; i++) {
            String productName = "Product" + (rand.nextInt(10) + 1);
            int milkType;
            int milkSelector = rand.nextInt(10);
            if (milkSelector < 2) {
                milkType = 0;
            } else if (milkSelector < 5) {
                milkType = 5;
            } else {
                milkType = rand.nextInt(4) + 1;
            }
            
            String message = "{"
                    + "\"product_name\":\"" + productName + "\","
                    + "\"size\":\"" + (rand.nextBoolean() ? "short" : "tall") + "\","
                    + "\"milk\":" + milkType + ","
                    + "\"whip\":" + rand.nextInt(2) + ","
                    + "\"serv_size_m_l\":" + (rand.nextInt(500) + 100) + ","
                    + "\"calories\":" + rand.nextInt(300) + ","
                    + "\"total_fat_g\":" + rand.nextDouble() + ","
                    + "\"saturated_fat_g\":" + rand.nextDouble() + ","
                    + "\"trans_fat_g\":" + rand.nextDouble() + ","
                    + "\"cholesterol_mg\":" + rand.nextInt(50) + ","
                    + "\"sodium_mg\":" + rand.nextInt(200) + ","
                    + "\"total_carbs_g\":" + rand.nextInt(100) + ","
                    + "\"fiber_g\":" + rand.nextDouble() + ","
                    + "\"sugar_g\":" + rand.nextInt(50) + ","
                    + "\"caffeine_mg\":" + rand.nextInt(200)
                    + "}";
            ProducerRecord<String, String> record = new ProducerRecord<>("starbucks_data", "key-" + i, message);
            producer.send(record);
        }
        */
        // ... existing code ...

        producer.close();
        System.out.println("Data sent to Kafka topic.");
    }
}
