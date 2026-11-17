package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Test;

class CheckpointIntervalTest {

    @Test
    void checkpointingIsOffWithoutTheFlag() {
        Duration interval = TradingTrendsJob.checkpointInterval(ParameterTool.fromArgs(new String[] {}));
        assertEquals(Duration.ZERO, interval);
        StreamExecutionEnvironment env = TradingTrendsJob.environment(Variant.BASELINE, interval);
        assertFalse(env.getCheckpointConfig().isCheckpointingEnabled());
    }

    @Test
    void theFlagSetsTheCheckpointInterval() {
        Duration interval = TradingTrendsJob.checkpointInterval(
                ParameterTool.fromArgs(new String[] {"--checkpoint-interval-ms", "10000"}));
        StreamExecutionEnvironment env = TradingTrendsJob.environment(Variant.BASELINE, interval);
        assertTrue(env.getCheckpointConfig().isCheckpointingEnabled());
        assertEquals(10_000, env.getCheckpointConfig().getCheckpointInterval());
    }

    @Test
    void zeroTurnsCheckpointingOff() {
        Duration interval = TradingTrendsJob.checkpointInterval(
                ParameterTool.fromArgs(new String[] {"--checkpoint-interval-ms", "0"}));
        assertFalse(TradingTrendsJob.environment(Variant.BASELINE, interval)
                .getCheckpointConfig().isCheckpointingEnabled());
    }

    @Test
    void aNegativeIntervalIsRejected() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TradingTrendsJob.checkpointInterval(
                        ParameterTool.fromArgs(new String[] {"--checkpoint-interval-ms", "-1"})));
        assertEquals("--checkpoint-interval-ms must be 0 (off) or positive: -1", error.getMessage());
    }
}
