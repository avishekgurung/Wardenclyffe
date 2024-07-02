package avishek.kafka.labs.plain;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class PlainProducer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(PlainProducer.class);

    public static void main(String[] args) {
        /**
         * We are sending a plain text which is as string. The Codec properties set is for that of a String.
         */

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(Configuration.DEFAULT_TOPIC, "Hellow World1");
        producer.send(record);
        producer.flush();
        producer.close();
        log.info("Record produced {}", record);
    }
}
