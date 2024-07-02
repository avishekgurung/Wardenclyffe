package avishek.kafka.labs.consumer.groups;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

public class HashPartitioning {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(HashPartitioning.class);
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String[] names = {"Branson", "Marley", "Levi", "Reyna", "August", "Santiago", "Kellen"};
        for(int i=0; i < 10000; i++) {
            String key = names[new Random().nextInt(names.length)];
            String value = key + "-" + i;
            int paritionCount = 3;
            int partition = Math.abs(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % (paritionCount - 1);
            ProducerRecord<String, String> record = new ProducerRecord<>(Configuration.MYNTRA_TOPIC,  key, value);
            producer.send(record);
            log.info("Record published {} to partiton {}", record, partition);
            Thread.sleep(100);
        }
        producer.flush();
        producer.close();
    }
}
