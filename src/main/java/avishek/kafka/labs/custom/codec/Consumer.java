package avishek.kafka.labs.custom.codec;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(Consumer.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Configuration.AUTO_OFFSET_RESET_CONFIG_EARLIEST);

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(Configuration.DEFAULT_TOPIC));

        log.info("Codec Consumer started for topic: {} and consumer group: {}", Configuration.DEFAULT_TOPIC,
                Configuration.GROUP_ID);
        while(true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, User> record : records) {
                log.info("Key : {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(),
                        record.partition(), record.offset());
            }
        }

        /**
         * A simple consumer that just uses String as codec also consumes and displays the message.
         */
    }
}
