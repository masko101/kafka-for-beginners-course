package mark.practice.kafka.tut1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignAndSeekDemo {

    public static Logger log = LoggerFactory.getLogger(ConsumerAssignAndSeekDemo.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerDemo.KAFKA_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition topicPartition = new TopicPartition(ProducerCallbackDemo.KAFKA_TOPIC, 0);
        consumer.assign(Arrays.asList(topicPartition));
        long offsetToReadFrom = 15l;
        consumer.seek(topicPartition, offsetToReadFrom);

        int numMessagesToRead = 5;
        int numMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                numMessagesReadSoFar++;
                log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                log.info(String.format("Partition: %d, Offsets: %d", record.partition(), record.offset()));
                if (numMessagesReadSoFar >= numMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
