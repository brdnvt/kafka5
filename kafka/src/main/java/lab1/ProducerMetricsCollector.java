package lab1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.gson.JsonObject;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Instant;

public class ProducerMetricsCollector {
    private static final String METRICS_TOPIC = "producer_metrics";
    private final KafkaProducer<String, String> producer;
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private long startTime;

    public ProducerMetricsCollector() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        this.producer = new KafkaProducer<>(props);
        this.startTime = System.currentTimeMillis();
    }

    public void recordMetrics(String message) {
        messageCount.incrementAndGet();
        bytesSent.addAndGet(message.getBytes().length);
        
        if (messageCount.get() % 10 == 0) { 
            sendMetrics();
        }
    }

    private void sendMetrics() {
        long currentTime = System.currentTimeMillis();
        double throughput = (double) messageCount.get() / ((currentTime - startTime) / 1000.0);
        
        JsonObject metrics = new JsonObject();
        metrics.addProperty("timestamp", Instant.now().toString());
        metrics.addProperty("total_messages", messageCount.get());
        metrics.addProperty("total_bytes", bytesSent.get());
        metrics.addProperty("messages_per_second", throughput);
        
        producer.send(new ProducerRecord<>(METRICS_TOPIC, "producer_metrics", metrics.toString()),
            (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending metrics: " + exception.getMessage());
                }
            });
    }

    public void close() {
        sendMetrics(); 
        producer.close();
    }
}
