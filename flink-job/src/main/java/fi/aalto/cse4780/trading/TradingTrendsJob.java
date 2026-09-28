package fi.aalto.cse4780.trading;

import java.time.Duration;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Query 1 job: {@code ticks} → parse and classify → per-symbol EMA38/EMA100 → {@code ema-results}.
 *
 * <pre>
 * flink run -d -c fi.aalto.cse4780.trading.TradingTrendsJob /opt/job/flink-job.jar [options]
 *
 *   --bootstrap-servers  kafka:9092
 *   --ticks-topic        ticks
 *   --ema-topic          ema-results
 *   --group-id           trading-trends
 *   --watermark-bound-ms 60000   bounded out-of-orderness (provisional until the EDA, D-5)
 *   --idleness-ms        30000   mark a partition idle after this much wall-clock silence; 0 = off
 *   --zone-offset        Z       offset of trading timestamps, e.g. +01:00 (provisional, D-3)
 *   --bounded                    stop at the offsets that exist at startup (finite replay)
 * </pre>
 *
 * <p>The job always reads {@code ticks} from the earliest offset, so reruns see the same input. With
 * {@code --bounded}, end of input advances the watermark to the maximum. This evaluates every
 * remaining window, including the last one.
 */
public class TradingTrendsJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap-servers", "kafka:9092");
        Duration watermarkBound = Duration.ofMillis(params.getLong("watermark-bound-ms", 60_000));
        Duration idleness = Duration.ofMillis(params.getLong("idleness-ms", 30_000));
        ZoneOffset zoneOffset = ZoneOffset.of(params.get("zone-offset", "Z"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSourceBuilder<Tick> source = KafkaSource.<Tick>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(params.get("ticks-topic", "ticks"))
                .setGroupId(params.get("group-id", "trading-trends"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TickDeserializer(new TickParser(zoneOffset)));
        if (params.has("bounded")) {
            source.setBounded(OffsetsInitializer.latest());
        }

        DataStream<Tick> ticks = env
                .fromSource(source.build(), watermarks(watermarkBound, idleness), "ticks")
                .uid("ticks-source");

        emaResults(ticks)
                .sinkTo(KafkaSink.<EmaResult>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                new EmaResultSerializer(params.get("ema-topic", "ema-results")))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("ema-results")
                .uid("ema-results-sink");

        env.execute("trading-trends");
    }

    /** Event time from the trading timestamp, with bounded out-of-orderness and optional idleness. */
    static WatermarkStrategy<Tick> watermarks(Duration outOfOrderness, Duration idleness) {
        WatermarkStrategy<Tick> strategy = WatermarkStrategy.<Tick>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner((tick, recordTimestamp) -> tick.eventTimeMillis());
        return idleness.isZero() ? strategy : strategy.withIdleness(idleness);
    }

    /** Query 1 on timestamped price events. */
    static SingleOutputStreamOperator<EmaResult> emaResults(DataStream<Tick> ticks) {
        return ticks
                .keyBy(tick -> tick.symbol)
                .process(new EmaCalculator())
                .name("q1-ema")
                .uid("q1-ema");
    }
}
