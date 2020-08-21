package mark.practice.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static final String KAFKA_SERVERS = "127.0.0.1:9092";
    public static final String KAFKA_TOPIC = "twit_twee_topic";

    static String API_CONSUMER_KEY = "VkzftRSjkdVl68e6abwhG1TFz";
    static String API_CONSUMER_SECRET = "FpG9l22EkvPd2co4q5Ay6qInaAGdncqZJ48zvHVHXzI4jtvfPM";
    static String API_TOKEN = "18718014-IOGECnk0gBawRdhzwfXPUDKAMgBOnVJ8yywynd5fp";
    static String API_SECRET = "MWEW1l6vKtrU5azgNZ3dQJgXQx1SccsNib2D0eglLlrZQ";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        KafkaProducer<String, String> twitPro = createKafkaProducer();

        Client hosebirdClient = createTwitterClient(msgQueue);
// Attempts to establish a connection.
        hosebirdClient.connect();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down application...");
            log.info("Shutting down twitter client");
            hosebirdClient.stop();
            log.info("Shutting down Kakfa producer");
            twitPro.close();
            log.info("Successfully shutdown application");
        }));
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                log.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(KAFKA_TOPIC, msg);
                twitPro.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                            log.error("Kablooey: ", e);
                    }
                });
            }

        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //SAFE config
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //HIGH performance config
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new KafkaProducer<String, String>(properties);
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);

        List<String> terms = Lists.newArrayList("sci-fi", "syfy", "scifi", "science fiction", "hot");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(API_CONSUMER_KEY, API_CONSUMER_SECRET, API_TOKEN, API_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        return builder.build();
    }
}
