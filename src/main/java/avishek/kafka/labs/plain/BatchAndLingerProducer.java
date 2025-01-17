package avishek.kafka.labs.plain;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class BatchAndLingerProducer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(BatchAndLingerProducer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i < 2; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(Configuration.DEFAULT_TOPIC, "Hi " + i);
            producer.send(record);
            log.info("Record produced {}", record);
        }
        producer.flush();
        producer.close();
    }
}
