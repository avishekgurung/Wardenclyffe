package avishek.kafka.labs.consumer.groups;

import avishek.kafka.labs.Configuration;
import avishek.kafka.labs.plain.PlainConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer1 {

    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(Consumer1.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.VORTA_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Configuration.AUTO_OFFSET_RESET_CONFIG_EARLIEST);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(Configuration.MYNTRA_TOPIC));
        log.info("Consumer-1 started for topic: '{}' with group: '{}'", Configuration.MYNTRA_TOPIC, Configuration.VORTA_CONSUMER_GROUP);
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                log.info("Key : {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(),
                        record.partition(), record.offset());
            }
        }
    }
}
