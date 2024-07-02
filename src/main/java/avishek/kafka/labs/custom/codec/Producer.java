package avishek.kafka.labs.custom.codec;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class Producer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(Producer.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());

        User user = new User("Samip", 7);
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, User> record = new ProducerRecord<>(Configuration.DEFAULT_TOPIC, user);
        producer.send(record);
        producer.flush();
        producer.close();
        log.info("Record produced {}", record);
    }
}
