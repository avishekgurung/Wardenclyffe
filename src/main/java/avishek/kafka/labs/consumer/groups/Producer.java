package avishek.kafka.labs.consumer.groups;

import avishek.kafka.labs.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.Random;

public class Producer {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(Producer.class);
    public static void main(String[] args) throws Exception {
        /**
         * We are sending a plain text which is as string. The Codec properties set is for that of a String.
         */

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String[] names = {"Branson", "Marley", "Levi", "Reyna", "August", "Santiago", "Kellen"};
        for(int i=0; i < 10000; i++) {
            String value = names[new Random().nextInt(names.length)] + "-" + i;

            /**
             * Whenever, we do not use any key, then it is the default partitioning. For Kafka <= 2.3,
             * the partitioning is Round Robin where, a record goes to different partitions in a RR fashion.
             * However, for Kafka >= 2.4, the default partitioning is Sticky. It will send the record to only one
             * specific partition, since sticky partition improves the performance.
             */
            ProducerRecord<String, String> record = new ProducerRecord<>(Configuration.MYNTRA_TOPIC, value);
            producer.send(record);
            log.info("Record published {}", record);
            //Thread.sleep(100);
        }
        producer.flush();
        producer.close();

    }
}