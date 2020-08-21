package mark.practice.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static final String KAFKA_TOPIC = "twit_twee_topic";

    public static void main(String[] args) {
        Properties  properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty( StreamsConfig.APPLICATION_ID_CONFIG , "kafka-demo-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder  = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream(KAFKA_TOPIC);
        KStream<String, String> filter = kStream.filter((key, tweet) -> extractTwitterIdFromRecord(tweet) > 10000);
        filter.to("important_tweets");

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
    }

    private static int extractTwitterIdFromRecord(String tweet) {
        try {
            return JsonParser.parseString(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            System.out.println("ZOOOOOOP");
            return 0;
        }
    }

}
