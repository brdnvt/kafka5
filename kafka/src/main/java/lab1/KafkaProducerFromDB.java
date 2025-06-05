package lab1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.util.Properties;

public class KafkaProducerFromDB {
    private static final String USER = "postgres_user";
    private static final String PASSWORD = "postgres_password";
    public static void main(String[] args) {

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);

        String jdbcUrl = "jdbc:postgresql://localhost:5430/postgres_db";


        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            String query = "SELECT * FROM coffee_products";
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String productName = resultSet.getString("product_name");
                String size = resultSet.getString("size");
                int milk = resultSet.getInt("milk");
                int whip = resultSet.getInt("whip");
                int servSize = resultSet.getInt("serv_size_m_l");
                int calories = resultSet.getInt("calories");

                String message = "{"
                        + "\"product_name\":\"" + productName + "\","
                        + "\"size\":\"" + size + "\","
                        + "\"milk\":" + milk + ","
                        + "\"whip\":" + whip + ","
                        + "\"serv_size_m_l\":" + servSize + ","
                        + "\"calories\":" + calories
                        + "}";

                producer.send(new ProducerRecord<>("coffee_products", productName, message));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

        System.out.println("Data sent to Kafka.");
    }
}
