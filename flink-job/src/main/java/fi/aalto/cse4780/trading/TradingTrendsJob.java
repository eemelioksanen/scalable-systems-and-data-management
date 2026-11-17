package fi.aalto.cse4780.trading;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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
 *   --watermark-bound-ms  1000    bounded out-of-orderness; 64265001 with --bounded (D-38)
 *   --idleness-ms         30000   mark a partition idle after this much wall-clock silence,
 *                                 0 = off; 0 with --bounded (D-38)
 *   --zone-offset         +02:00  offset of trading timestamps: literal CEST (D-3, D-32)
 *   --bounded                     stop both inputs at their startup offsets (finite replay)
 *
 *   Design-comparison variants (D-42); each default is the baseline:
 *   --operator            process  process | window: Query 1 as EmaCalculator or Flink windows
 *   --empty-windows       skip     skip | carry-forward: empty windows (D-6)
 *   --state-backend       hashmap  hashmap | rocksdb
 *   --ticks-encoding      csv      csv | binary: must match the replayer's --encoding
 * </pre>
 *
 * <p>The job always reads {@code ticks} and {@code subscriptions} from the earliest offset, so
 * reruns see the same input. With {@code --bounded}, the input ends at the startup offsets, and
 * subscription changes written after submission are not read. End of input, there or on
 * {@code flink stop --drain}, evaluates every remaining window, including the last one.
 *
 * <p>A price event whose window has already been evaluated is late: it is counted in
 * {@code lateTicks}, goes to the {@code late-ticks} side output and is left out of the
 * calculation. The watermark defaults decide which events those are, and they differ by mode
 * (D-38): see {@link #LIVE} and {@link #FINITE_REPLAY}.
 *
 * <p>The job sets its effective configuration, defaults included, as the global job parameters,
 * so Flink's REST API shows it under {@code /jobs/<id>/config}.
 */
public class TradingTrendsJob {

    /**
     * Watermark defaults of a live job (D-38). A window is published once every partition is a
     * second of event time past its end, or has been silent for 30 s. The week needs 264 ms
     * outside one burst (D-30): the 5,506 summary rows of 10 November arrive about 18 h after
     * their window and are late under this bound.
     */
    static final Watermarking LIVE =
            new Watermarking(Duration.ofSeconds(1), Duration.ofSeconds(30));

    /**
     * Watermark defaults of a finite replay (D-38). The bound is the tested one that admits every
     * event of the week, the burst included (D-30). Idleness is off: it runs on wall-clock time,
     * so it could reject events in one run and not in the next, and end of input evaluates every
     * remaining window without it.
     */
    static final Watermarking FINITE_REPLAY =
            new Watermarking(Duration.ofMillis(64_265_001), Duration.ZERO);

    /** The out-of-orderness bound and the idleness timeout. Zero idleness turns it off. */
    record Watermarking(Duration bound, Duration idleness) {

        /** The flags, or for a missing one the default of the job's mode. */
        static Watermarking from(ParameterTool params) {
            Watermarking defaults = params.has("bounded") ? FINITE_REPLAY : LIVE;
            return new Watermarking(
                    Duration.ofMillis(
                            params.getLong("watermark-bound-ms", defaults.bound.toMillis())),
                    Duration.ofMillis(params.getLong("idleness-ms", defaults.idleness.toMillis())));
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap-servers", "kafka:9092");
        String groupId = params.get("group-id", "trading-trends");
        String subscriptionsTopic = params.get("subscriptions-topic", "subscriptions");
        Watermarking watermarking = Watermarking.from(params);
        ZoneOffset zoneOffset = ZoneOffset.of(params.get("zone-offset", "+02:00"));
        boolean bounded = params.has("bounded");
        Variant variant = Variant.from(params);

        Map<String, String> effective = new LinkedHashMap<>();
        effective.put("operator", variant.operator().flag());
        effective.put("emptyWindows", variant.emptyWindows().flag());
        effective.put("stateBackend", variant.stateBackend().flag());
        effective.put("ticksEncoding", variant.ticksEncoding().flag());
        effective.put("watermarkBoundMs", String.valueOf(watermarking.bound().toMillis()));
        effective.put("idlenessMs", String.valueOf(watermarking.idleness().toMillis()));
        effective.put("zoneOffset", zoneOffset.getId());
        effective.put("bounded", String.valueOf(bounded));

        StreamExecutionEnvironment env = environment(variant);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(effective));

        KafkaSourceBuilder<Tick> tickSource = KafkaSource.<Tick>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(params.get("ticks-topic", "ticks"))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TickDeserializer(
                        new TickParser(zoneOffset), variant.ticksEncoding()));
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
                .fromSource(
                        tickSource.build(),
                        watermarks(watermarking.bound(), watermarking.idleness()),
                        "ticks")
                .uid("ticks-source");
        DataStream<SubscriptionUpdate> subscriptions = env
                .fromSource(subscriptionSource.build(), WatermarkStrategy.noWatermarks(),
                        "subscriptions")
                .setParallelism(1)
                .uid("subscriptions-source");

        SingleOutputStreamOperator<EmaResult> results = emaResults(ticks, variant);
        results.getSideOutput(EmaCalculator.LATE_TICKS)
                .sinkTo(sink(bootstrapServers,
                        new LateTickSerializer(params.get("late-topic", "late-ticks"))))
                .name("late-ticks")
                .disableChaining()
                .uid("late-ticks-sink");
        DataStream<EmaResult> published =
                published(results, subscriptions, subscriptionsEnd.nonEmpty());
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

    /** The execution environment, with {@code variant}'s state backend. */
    static StreamExecutionEnvironment environment(Variant variant) {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, variant.stateBackend().flag());
        return StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    /** Event time from the trading timestamp, with bounded out-of-orderness and optional idleness. */
    static WatermarkStrategy<Tick> watermarks(Duration outOfOrderness, Duration idleness) {
        WatermarkStrategy<Tick> strategy = WatermarkStrategy.<Tick>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner((tick, recordTimestamp) -> tick.eventTimeMillis);
        return idleness.isZero() ? strategy : strategy.withIdleness(idleness);
    }

    /**
     * Query 1 and Query 2 on timestamped price events, for every symbol, built as
     * {@code variant}'s operator. End of input evaluates the windows still open, whether or not
     * the sources were idle by then. Either way the keyed operator is named {@code q1-ema}, so
     * its metrics keep their names, and late price events leave it as {@link
     * EmaCalculator#LATE_TICKS}.
     */
    static SingleOutputStreamOperator<EmaResult> emaResults(DataStream<Tick> ticks, Variant variant) {
        OneInputStreamOperator<Tick, EmaResult> operator = switch (variant.operator()) {
            case PROCESS -> new EndOfInputProcessOperator<>(new EmaCalculator(variant.emptyWindows()));
            case WINDOW -> new EndOfInputWindowOperator(
                    variant.emptyWindows(), ticks.getExecutionConfig().getSerializerConfig());
        };
        return ticks
                .keyBy(tick -> tick.symbol)
                .transform("q1-ema", TypeInformation.of(EmaResult.class), operator)
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

    private static <T> KafkaSink<T> sink(
            String bootstrapServers, KafkaRecordSerializationSchema<T> serializer) {
        return KafkaSink.<T>builder()
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
