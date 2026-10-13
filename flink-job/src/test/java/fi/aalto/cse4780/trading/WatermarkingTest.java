package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.clock.RelativeClock;
import org.junit.jupiter.api.Test;

/** The watermark defaults of each mode (D-38), and what a partition's generator does with them. */
class WatermarkingTest {

    private static final long SECOND_NANOS = 1_000_000_000L;

    /** Wall-clock time as the idleness timer sees it. Never 0, which the timer reads as unset. */
    private long nanos = SECOND_NANOS;
    private final Output output = new Output();

    @Test
    void aLiveJobDefaultsToTheMeasuredBoundWithIdleness() {
        TradingTrendsJob.Watermarking live = watermarking();

        assertEquals(Duration.ofSeconds(1), live.bound());
        assertEquals(Duration.ofSeconds(30), live.idleness());
    }

    @Test
    void aFiniteReplayDefaultsToTheAdmitAllBoundWithoutIdleness() {
        TradingTrendsJob.Watermarking replay = watermarking("--bounded");

        assertEquals(Duration.ofMillis(64_265_001), replay.bound());
        assertEquals(Duration.ZERO, replay.idleness());
    }

    @Test
    void aFlagReplacesOnlyItsOwnDefaultInEitherMode() {
        assertEquals(
                new TradingTrendsJob.Watermarking(Duration.ofSeconds(60), Duration.ofSeconds(30)),
                watermarking("--watermark-bound-ms", "60000"));
        assertEquals(
                new TradingTrendsJob.Watermarking(Duration.ofSeconds(1), Duration.ZERO),
                watermarking("--idleness-ms", "0"));
        assertEquals(
                new TradingTrendsJob.Watermarking(Duration.ofSeconds(1), Duration.ZERO),
                watermarking("--bounded", "--watermark-bound-ms", "1000"));
        assertEquals(
                new TradingTrendsJob.Watermarking(Duration.ofMillis(64_265_001), Duration.ofSeconds(5)),
                watermarking("--idleness-ms", "5000", "--bounded"));
    }

    @Test
    void theWatermarkTrailsTheLatestEventTimeByTheBound() {
        WatermarkGenerator<Tick> generator = generator(Duration.ofSeconds(1), Duration.ZERO);

        generator.onEvent(tick(10_000), 10_000, output);
        generator.onEvent(tick(9_500), 9_500, output);
        generator.onPeriodicEmit(output);

        // An event at or before 8,999 would now be out of order by more than the bound.
        assertEquals(8_999, output.watermark);
    }

    @Test
    void aPartitionSilentForTheTimeoutGoesIdleUntilItsNextEvent() {
        WatermarkGenerator<Tick> generator =
                generator(Duration.ofSeconds(1), Duration.ofSeconds(30));
        generator.onEvent(tick(10_000), 10_000, output);
        generator.onPeriodicEmit(output);

        generator.onPeriodicEmit(output);
        nanos += 30 * SECOND_NANOS;
        generator.onPeriodicEmit(output);
        assertFalse(output.idle, "30 s of silence is not yet more than the timeout");

        nanos += 1;
        generator.onPeriodicEmit(output);
        assertTrue(output.idle);

        // An idle partition no longer holds the job's watermark back, so its next event is judged
        // against the other partitions' progress (D-28). Its own watermark starts again here.
        generator.onEvent(tick(400_000), 400_000, output);
        generator.onPeriodicEmit(output);
        assertFalse(output.idle);
        assertEquals(398_999, output.watermark);
    }

    @Test
    void withoutIdlenessASilentPartitionKeepsHoldingTheWatermark() {
        WatermarkGenerator<Tick> generator = generator(Duration.ofSeconds(1), Duration.ZERO);
        generator.onEvent(tick(10_000), 10_000, output);
        generator.onPeriodicEmit(output);

        generator.onPeriodicEmit(output);
        nanos += 24 * 3600 * SECOND_NANOS;
        generator.onPeriodicEmit(output);

        assertFalse(output.idle);
        assertEquals(8_999, output.watermark);
    }

    private static TradingTrendsJob.Watermarking watermarking(String... args) {
        return TradingTrendsJob.Watermarking.from(ParameterTool.fromArgs(args));
    }

    private WatermarkGenerator<Tick> generator(Duration bound, Duration idleness) {
        RelativeClock clock = new RelativeClock() {
            @Override
            public long relativeTimeMillis() {
                return nanos / 1_000_000;
            }

            @Override
            public long relativeTimeNanos() {
                return nanos;
            }
        };
        return TradingTrendsJob.watermarks(bound, idleness)
                .createWatermarkGenerator(new WatermarkGeneratorSupplier.Context() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public RelativeClock getInputActivityClock() {
                        return clock;
                    }
                });
    }

    private static Tick tick(long eventTimeMillis) {
        return new Tick("RDSA.NL", "E", 20.0, eventTimeMillis, 0, 0);
    }

    /** What one partition's generator last told the source: its watermark, or that it is idle. */
    private static final class Output implements WatermarkOutput {

        private long watermark = Long.MIN_VALUE;
        private boolean idle;

        @Override
        public void emitWatermark(Watermark emitted) {
            watermark = emitted.getTimestamp();
            idle = false;
        }

        @Override
        public void markIdle() {
            idle = true;
        }

        @Override
        public void markActive() {
            idle = false;
        }
    }
}
