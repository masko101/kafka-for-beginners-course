package mark.practice.kafka.tut1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreading {

    public static Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreading.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThreading().run();
    }

    private ConsumerDemoWithThreading() {
    }

    public void run() {
        log.info("Creating consumer thread");
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable myConsumerThread = new ConsumerRunnable(ProducerDemo.KAFKA_SERVERS, "my-6th-app",
                ProducerCallbackDemo.KAFKA_TOPIC, latch);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            myConsumerThread.shutdown();
        }));
        try {
            latch.await();
        } catch (InterruptedException ie) {
            log.error("Application got interrupted", ie);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String serversConfig, String group, String topic, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serversConfig);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer(properties);
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                        log.info(String.format("Partition: %d, Offsets: %d", record.partition(), record.offset()));
                    }
                }
            } catch (WakeupException e) {
                log.info("Recieved Shutdown Signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
