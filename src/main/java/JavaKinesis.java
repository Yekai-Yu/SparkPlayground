import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.examples.streaming.KinesisExampleUtils;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

public class JavaKinesis {
    private static final Pattern WORD_SEPARATOR = Pattern.compile(",");

    public static void main(String[] args) throws Exception {
        // Check that all required args were passed in.
        if (args.length != 3) {
            System.err.println(
                    "Usage: JavaKinesisWordCountASL <stream-name> <endpoint-url>\n\n" +
                            "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
                            "    <stream-name> is the name of the Kinesis stream\n" +
                            "    <endpoint-url> is the endpoint of the Kinesis service\n" +
                            "                   (e.g. https://kinesis.us-east-1.amazonaws.com)\n" +
                            "Generate data for the Kinesis stream using the example KinesisWordProducerASL.\n" +
                            "See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more\n" +
                            "details.\n"
            );
            System.exit(1);
        }

        // Set default log4j logging level to WARN to hide Spark logs
        StreamingExamples.setStreamingLogLevels();

        // Populate the appropriate variables from the given args
        String kinesisAppName = args[0];
        String streamName = args[1];
        String endpointUrl = args[2];

        // Create a Kinesis client in order to determine the number of shards for the given stream
        AmazonKinesisClient kinesisClient =
                new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
        kinesisClient.setEndpoint(endpointUrl);
        int numShards =
                kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();


        // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
        // This is not a necessity; if there are less receivers/DStreams than the number of shards,
        // then the shards will be automatically distributed among the receivers and each receiver
        // will receive data from multiple shards.
        int numStreams = numShards;

        // Spark Streaming batch interval
        Duration batchInterval = new Duration(2000);

        // Kinesis checkpoint interval.  Same as batchInterval for this example.
        Duration kinesisCheckpointInterval = batchInterval;

        // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
        // DynamoDB of the same region as the Kinesis stream
        String regionName = KinesisExampleUtils.getRegionNameByEndpoint(endpointUrl);

        // Setup the Spark config and StreamingContext
        SparkConf sparkConfig = new SparkConf().setAppName("JavaKinesis");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

        // Create the Kinesis DStreams
        List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            streamsList.add(JavaDStream.fromDStream(
                    KinesisInputDStream.builder()
                            .streamingContext(jssc)
                            .checkpointAppName(kinesisAppName)
                            .streamName(streamName)
                            .endpointUrl(endpointUrl)
                            .regionName(regionName)
                            .initialPosition(new KinesisInitialPositions.Latest())
                            .checkpointInterval(kinesisCheckpointInterval)
                            .storageLevel(StorageLevel.MEMORY_AND_DISK_2())
                            .build(),
                    ClassTag$.MODULE$.apply(byte[].class)
            ));
        }

        // Union all the streams if there is more than 1 stream
        JavaDStream<byte[]> unionStreams = streamsList.get(0);
//        if (streamsList.size() > 1) {
//            unionStreams = jssc.union(streamsList.toArray(new JavaDStream[0]));
//        } else {
//            // Otherwise, just use the 1 stream
//            unionStreams = streamsList.get(0);
//        }

        // Convert each line of Array[Byte] to String, and split into words
        JavaDStream<String> words = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
            @Override
            public Iterator<String> call(byte[] line) {
                String s = new String(line, StandardCharsets.UTF_8);
                return Arrays.asList(WORD_SEPARATOR.split(s)).iterator();
            }
        });

        // Map each word to a (word, 1) tuple so we can reduce by key to count the words
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                }
        );

        // Print the first 10 wordCounts
        wordCounts.print();

        // Start the streaming context and await termination
        jssc.start();
        jssc.awaitTermination();
    }
}