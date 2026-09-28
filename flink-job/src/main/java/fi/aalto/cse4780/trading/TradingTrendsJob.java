package fi.aalto.cse4780.trading;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * Query 1 and Query 2: {@code ticks} → parse and classify → per-symbol EMA38/EMA100 and crossovers
 * → subscription filter → {@code ema-results} and {@code advisories}.
 *
 * <pre>
 * flink run -d -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar [options]
 *
 *   --bootstrap-servers   kafka:9092
 *   --ticks-topic         ticks
 *   --subscriptions-topic subscriptions
 *   --ema-topic           ema-results
 *   --advisories-topic    advisories
 *   --group-id            trading-trends
 *   --watermark-bound-ms  60000   bounded out-of-orderness (provisional until the EDA, D-5)
 *   --idleness-ms         30000   mark a partition idle after this much wall-clock silence; 0 = off
 *   --zone-offset         Z       offset of trading timestamps, e.g. +01:00 (provisional, D-3)
 *   --bounded                     stop both inputs at their startup offsets (finite replay)
 * </pre>
 *
 * <p>The job always reads {@code ticks} and {@code subscriptions} from the earliest offset, so
 * reruns see the same input. With {@code --bounded}, end of input advances the watermark to the
 * maximum. This evaluates every remaining window, including the last one. Subscription changes
 * written after submission are then not read.
 */
public class TradingTrendsJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap-servers", "kafka:9092");
        String groupId = params.get("group-id", "trading-trends");
        String subscriptionsTopic = params.get("subscriptions-topic", "subscriptions");
        Duration watermarkBound = Duration.ofMillis(params.getLong("watermark-bound-ms", 60_000));
        Duration idleness = Duration.ofMillis(params.getLong("idleness-ms", 30_000));
        ZoneOffset zoneOffset = ZoneOffset.of(params.get("zone-offset", "Z"));
        boolean bounded = params.has("bounded");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceBuilder<Tick> tickSource = KafkaSource.<Tick>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(params.get("ticks-topic", "ticks"))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TickDeserializer(new TickParser(zoneOffset)));
        if (bounded) {
            tickSource.setBounded(OffsetsInitializer.latest());
        }

        TopicEnd subscriptionsEnd = topicEnd(bootstrapServers, subscriptionsTopic);
        KafkaSourceBuilder<SubscriptionUpdate> subscriptionSource =
                KafkaSource.<SubscriptionUpdate>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setTopics(subscriptionsTopic)
                        .setGroupId(groupId + "-subscriptions")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(new SubscriptionDeserializer());
        if (bounded) {
            subscriptionSource.setBounded(OffsetsInitializer.offsets(subscriptionsEnd.offsets()));
        }

        DataStream<Tick> ticks = env
                .fromSource(tickSource.build(), watermarks(watermarkBound, idleness), "ticks")
                .uid("ticks-source");
        DataStream<SubscriptionUpdate> subscriptions = env
                .fromSource(subscriptionSource.build(), WatermarkStrategy.noWatermarks(),
                        "subscriptions")
                .setParallelism(1)
                .uid("subscriptions-source");

        DataStream<EmaResult> published =
                published(emaResults(ticks), subscriptions, subscriptionsEnd.nonEmpty());
        published
                .sinkTo(sink(bootstrapServers,
                        new EmaResultSerializer(params.get("ema-topic", "ema-results"))))
                .name("ema-results")
                .uid("ema-results-sink");
        published
                .filter(result -> result.crossover != null)
                .name("crossovers")
                .uid("crossovers")
                .sinkTo(sink(bootstrapServers,
                        new AdvisorySerializer(params.get("advisories-topic", "advisories"))))
                .name("advisories")
                .uid("advisories-sink");

        env.execute("trading-trends");
    }

    /** Event time from the trading timestamp, with bounded out-of-orderness and optional idleness. */
    static WatermarkStrategy<Tick> watermarks(Duration outOfOrderness, Duration idleness) {
        WatermarkStrategy<Tick> strategy = WatermarkStrategy.<Tick>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner((tick, recordTimestamp) -> tick.eventTimeMillis);
        return idleness.isZero() ? strategy : strategy.withIdleness(idleness);
    }

    /** Query 1 and Query 2 on timestamped price events, for every symbol. */
    static SingleOutputStreamOperator<EmaResult> emaResults(DataStream<Tick> ticks) {
        return ticks
                .keyBy(tick -> tick.symbol)
                .process(new EmaCalculator())
                .name("q1-ema")
                .uid("q1-ema");
    }

    /**
     * The results of subscribed symbols (D-9).
     *
     * @param startupEndOffsets see {@link SubscriptionFilter#SubscriptionFilter(Map)}
     */
    static SingleOutputStreamOperator<EmaResult> published(
            DataStream<EmaResult> results,
            DataStream<SubscriptionUpdate> subscriptions,
            Map<Integer, Long> startupEndOffsets) {
        return results
                .keyBy(result -> result.symbol)
                .connect(subscriptions.broadcast(SubscriptionFilter.TRADERS,
                        SubscriptionFilter.SUBSCRIBED, SubscriptionFilter.POSITION))
                .process(new SubscriptionFilter(startupEndOffsets))
                .name("subscription-filter")
                .uid("subscription-filter");
    }

    private static KafkaSink<EmaResult> sink(
            String bootstrapServers, KafkaRecordSerializationSchema<EmaResult> serializer) {
        return KafkaSink.<EmaResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * A topic's end offsets: {@code offsets} for every partition, {@code nonEmpty} only for the
     * partitions that hold records.
     */
    record TopicEnd(Map<TopicPartition, Long> offsets, Map<Integer, Long> nonEmpty) {}

    /** Reads {@code topic}'s end offsets now, at job submission. */
    static TopicEnd topicEnd(String bootstrapServers, String topic) throws Exception {
        try (Admin admin = Admin.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            TopicDescription description =
                    admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (TopicPartitionInfo partition : description.partitions()) {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                earliest.put(tp, OffsetSpec.earliest());
                latest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResultInfo> starts =
                    admin.listOffsets(earliest).all().get();
            Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(latest).all().get();

            Map<TopicPartition, Long> offsets = new HashMap<>();
            Map<Integer, Long> nonEmpty = new HashMap<>();
            for (TopicPartition tp : latest.keySet()) {
                long end = ends.get(tp).offset();
                offsets.put(tp, end);
                if (end > starts.get(tp).offset()) {
                    nonEmpty.put(tp.partition(), end);
                }
            }
            return new TopicEnd(offsets, nonEmpty);
        }
    }
}
