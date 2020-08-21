package mark.practice.kafka.tut1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerCallbackDemo {

    public static final String KAFKA_TOPIC = "first_topic";
    public static Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);
    public static final String KAFKA_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 15; i++) {
            String value = "JAVA_ZOOP #" + i;
            String key = "KEE #" + i;

            logger.info("Key: " + key);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(KAFKA_TOPIC, key, value);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info(String.format("Recv new metadata\n Topic: %s\n Partition: %s\n Offsets: %s\n " +
                                        "Timestamp: %s\n", recordMetadata.topic(), recordMetadata.partition(),
                                recordMetadata.offset(), recordMetadata.timestamp()));
                    } else {
                        logger.error("Error producing", e);
                    }

                }
            }).get();
        }

        producer.flush();
        producer.close();
     }
}
