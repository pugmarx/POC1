package org.pgmx.cloud.poc.poc1;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * /*
 * Demonstrates fetching records from a Kafka topic, processing the records, and persisting them to Cassandra
 *
 *
 * <strong>Prerequisites</strong>
 * <ul>
 *     <li> Kafka running on localhost with 'testspark' topic
 *     <li> Cassandra running on localhost with 'cloudtest' keyspace
 * </ul>
 *
 * <em>Usage:</em>
 *
 * `$ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0
 * --class org.pgmx.cloud.poc.poc1.KafkaReaderTest target/poc1-1.0.jar`
 *
 */

public final class KafkaReaderTest {

    private static final Logger LOG = Logger.getLogger(KafkaReaderTest.class);

    private static final String ZK_HOST = "localhost:2181"; // FIXME
    private static final String IN_TOPIC = "testspark"; // FIXME
    private static final String IN_GROUP = "spark_" + UUID.randomUUID(); //FIXME
    private static final String NUM_THREADS = "1";
    private static final String CHECKPOINT_DIR = "/tmp/spark_checkpoints";
    private static final String STREAMING_JOB_COUNT = "10";
    private static final int FETCH_COUNT_INTERVAL = 20000; // FIXME
    private static final String MASTER_STRING = "local[*]";

    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern COMMA = Pattern.compile(",");

    private KafkaReaderTest() {
    }

    public static void main(String[] args) throws Exception {

        try {
            SparkConf sparkConf = new SparkConf().setAppName("KafkaReaderTest2").setMaster(MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(FETCH_COUNT_INTERVAL)); //FIXME duration
            jssc.checkpoint(CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            JavaPairReceiverInputDStream<String, String> messages =
                    KafkaUtils.createStream(jssc, ZK_HOST, IN_GROUP, topicMap);

            // Assuming that this gets the records from the message RDD
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // FIXME, remove union
            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndex(6));
            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndex(7));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);
            // FIXME, end

            //JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);
            JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);

            summarized.foreachRDD(rdd -> {
                if (rdd.count() > 0) {
                    System.out.println("----------->" + rdd.count());
                } else {
                    System.out.println("======== NOTHING IN RDD =============");
                }
            });

            // Store in a stateful ds
            JavaPairDStream<String, Integer> statefulMap = summarized.updateStateByKey(COMPUTE_RUNNING_SUM);

            statefulMap.print();
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // List of incoming vals, currentVal, returnVal
    private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>>
            COMPUTE_RUNNING_SUM = (nums, current) -> {
        int sum = current.orElse(0);
        for (int i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };

    static class RelevantIndex implements PairFunction<String, String, Integer> {
        int relIndex = 0;

        @SuppressWarnings("unused")
        public RelevantIndex() {
        }

        public RelevantIndex(int index) {
            relIndex = index;
        }

        public Tuple2<String, Integer> apply(String line) {
            return new Tuple2(line.split(",")[relIndex], Integer.valueOf(1));
        }

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2(s.split(",")[relIndex], Integer.valueOf(1));
        }
    }


    private static void persistInDB(JavaPairDStream<String, Integer> javaDStream, Class clazz, SparkConf conf, String org) {
        LOG.info("- Will save in DB table: " + clazz.getSimpleName() + " -");
        String keySpace = StringUtils.lowerCase("CloudPOC");
        String tableName = StringUtils.lowerCase(clazz.getSimpleName());
        String delQuery = "DELETE FROM " + keySpace + "." + tableName + " WHERE origin='" + org + "'";

        CassandraConnector connector = CassandraConnector.apply(conf);
        try (Session session = connector.openSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {'class': 'SimpleStrategy', " +
                    "'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + keySpace + "." + tableName
                    + " (airport text, freq double, primary key(airport))");

//            javaDStream.foreachRDD(rdd -> {
//                if (rdd.count() > 0) {
//                    session.execute(delQuery);
//                }
//            });

            Map<String, String> fieldToColumnMapping = new HashMap<>();
            fieldToColumnMapping.put("airport", "airport");
            fieldToColumnMapping.put("count", "freq");
            //CassandraStreamingJavaUtil.javaFunctions(javaDStream).writerBuilder(keySpace, tableName,
            //        CassandraJavaUtil.mapToRow(OriginDestDepDelayKey.class, fieldToColumnMapping)).saveToCassandra();
        }
    }

}
