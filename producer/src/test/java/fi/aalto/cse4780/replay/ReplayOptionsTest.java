package fi.aalto.cse4780.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayOptionsTest {

    private static final Instant NOW = Instant.parse("2026-09-28T18:30:05Z");

    @TempDir
    Path dir;

    private static ReplayOptions parse(String... args) throws IOException {
        return ReplayOptions.parse(args, Map.of(), NOW);
    }

    @Test
    void defaultsToAPreloadOfEverythingIntoTicks() throws IOException {
        ReplayOptions options = parse("--input", "/data");

        assertEquals(List.of(Path.of("/data")), options.inputs());
        assertEquals(ReplayOptions.Mode.PRELOAD, options.mode());
        assertEquals("ticks", options.topic());
        assertEquals("localhost:29092", options.bootstrapServers());
        assertTrue(options.exchanges().isEmpty());
        assertTrue(options.symbols().isEmpty());
        assertEquals(Long.MAX_VALUE, options.limit());
        assertEquals("replay-20260928T183005Z", options.runId());
        assertFalse(options.paced());
        assertNull(options.report());
    }

    @Test
    void takesTheBootstrapServersFromTheEnvironment() throws IOException {
        ReplayOptions options =
                ReplayOptions.parse(new String[] {"--input", "/data"}, Map.of("BOOTSTRAP_SERVERS", "kafka:9092"), NOW);
        assertEquals("kafka:9092", options.bootstrapServers());
    }

    @Test
    void parsesARealTimeSubsetReplay() throws IOException {
        Path symbols = Files.writeString(dir.resolve("symbols.txt"), "# traders' symbols\nRDSA.NL\n\n AIR.FR \n");
        ReplayOptions options = parse("--input", "a.csv", "--input", "b.csv", "--mode", "realtime", "--rate", "10",
                "--pace-from", "2021-11-08T09:00", "--exchanges", "FR,NL", "--symbols-file", symbols.toString(),
                "--limit", "1000", "--producer-property", "compression.type=zstd", "--report", "out.json");

        assertEquals(List.of(Path.of("a.csv"), Path.of("b.csv")), options.inputs());
        assertTrue(options.paced());
        assertEquals(10, options.rate());
        assertEquals(SourceClockTest.at("2021-11-08T09:00"), options.paceFromMillis());
        assertEquals(Set.of("FR", "NL"), options.exchanges());
        assertEquals(Set.of("RDSA.NL", "AIR.FR"), options.symbols());
        assertEquals(1000, options.limit());
        assertEquals(Map.of("compression.type", "zstd"), options.producerProperties());
        assertEquals(Path.of("out.json"), options.report());
    }

    @Test
    void rejectsInconsistentOptions() {
        assertThrows(IllegalArgumentException.class, () -> parse());
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--rate", "10"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--mode", "fast"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--partitions", "4"));
        assertThrows(IllegalArgumentException.class,
                () -> parse("--input", "a.csv", "--mode", "realtime", "--rate", "0"));
        assertThrows(IllegalArgumentException.class,
                () -> parse("--input", "a.csv", "--mode", "realtime", "--pace-from", "09:00"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--limit", "-1"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--producer-property", "acks"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input"));
        assertThrows(IllegalArgumentException.class, () -> parse("--input", "a.csv", "--verbose"));
    }

    @Test
    void preloadBatchesLargerThanLiveModes() throws IOException {
        Map<String, String> preload = Replayer.producerConfig(parse("--input", "a.csv"));
        Map<String, String> live = Replayer.producerConfig(parse("--input", "a.csv", "--mode", "max"));
        Map<String, String> overridden = Replayer.producerConfig(
                parse("--input", "a.csv", "--producer-property", "linger.ms=0"));

        assertEquals("all", preload.get("acks"));
        assertEquals("true", preload.get("enable.idempotence"));
        assertEquals("50", preload.get("linger.ms"));
        assertNull(live.get("linger.ms"));
        assertEquals("all", live.get("acks"));
        assertEquals("lz4", live.get("compression.type"));
        assertEquals("0", overridden.get("linger.ms"));
    }
}
