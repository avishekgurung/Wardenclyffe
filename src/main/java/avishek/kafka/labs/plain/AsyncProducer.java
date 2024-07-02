package avishek.kafka.labs.plain;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class AsyncProducer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(AsyncProducer.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);
        for(int i=0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(Configuration.DEFAULT_TOPIC, "Message-"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Record sent successfully: {}", record);
                    } else {
                        log.error("Error encountered for record: {}", record);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
