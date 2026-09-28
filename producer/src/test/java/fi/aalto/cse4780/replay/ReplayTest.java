package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.TestCsv.FIRST_DATA_LINE;
import static fi.aalto.cse4780.replay.TestCsv.price;
import static fi.aalto.cse4780.replay.TestCsv.quote;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayTest {

    static final int PARTITIONS = 8;
    private static final long WALL_START = Instant.parse("2026-09-28T18:00:00Z").toEpochMilli();

    @TempDir
    Path dir;

    private final AtomicLong wallMillis = new AtomicLong(WALL_START);
    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

    static Cluster cluster(String topic) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int p = 0; p < PARTITIONS; p++) {
            partitions.add(new PartitionInfo(topic, p, node, new Node[] {node}, new Node[] {node}));
        }
        return new Cluster("test", List.of(node), partitions, Set.of(), Set.of());
    }

    /** Kafka's rule for keyed records, which MockProducer does not apply by itself. */
    static class KeyHashPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                Cluster cluster) {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % cluster.partitionCountForTopic(topic);
        }

        @Override
        public void configure(Map<String, ?> configs) {}

        @Override
        public void close() {}
    }

    static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(cluster("ticks"), true, new KeyHashPartitioner(), new ByteArraySerializer(),
                new ByteArraySerializer());
    }

    static ReplayOptions options(String... args) throws IOException {
        return ReplayOptions.parse(args, Map.of(), Instant.parse("2026-09-28T18:00:00Z"));
    }

    private ReplayReport replay(MockProducer<byte[], byte[]> producer, ReplayOptions options) throws IOException {
        Replay replay = new Replay(options, InputFiles.resolve(options.inputs()), producer, PARTITIONS,
                Map.of("acks", "all"), () -> wallMillis.getAndIncrement(), nanos::get, nanos::addAndGet);
        return replay.run();
    }

    static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static int partitionOf(String symbol) {
        return Utils.toPositive(Utils.murmur2(symbol.getBytes(StandardCharsets.UTF_8))) % PARTITIONS;
    }

    Path week() throws IOException {
        return TestCsv.week(dir);
    }

    @Test
    void sendsEveryDataRowKeyedBySymbolInFileOrder() throws IOException {
        MockProducer<byte[], byte[]> producer = producer();
        ReplayReport report = replay(producer, options("--input", week().toString()));

        List<ProducerRecord<byte[], byte[]>> sent = producer.history();
        assertEquals(List.of("RDSA.NL", "AIR.FR", "AIR.FR", "846741.ETR", "RDSA.NL", "SIE.ETR"),
                sent.stream().map(r -> text(r.key())).toList());
        assertEquals("RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.123,", text(sent.get(0).value()));
        assertEquals("AIR.FR,E,08-11-2021,09:00:01.200,,,", text(sent.get(1).value()));

        // src = <file index>:<line>. The malformed row on line 16 is skipped, not renumbered.
        assertEquals(List.of("0:14", "0:15", "0:17", "0:18", "1:14", "1:15"), sent.stream()
                .map(r -> text(r.headers().lastHeader(Replay.SOURCE_HEADER).value())).toList());
        assertEquals(List.of(WALL_START + 1, WALL_START + 2, WALL_START + 3, WALL_START + 4, WALL_START + 5,
                WALL_START + 6), sent.stream().map(ProducerRecord::timestamp).toList());

        assertEquals(ReplayReport.Status.COMPLETE, report.status());
        ReplayReport.Counts counts = report.counts();
        assertEquals(2 * (FIRST_DATA_LINE - 1) + 7, counts.lines());
        assertEquals(2 * 12, counts.comments());
        assertEquals(2, counts.headers());
        assertEquals(1, counts.rejected());
        assertEquals(6, counts.dataRows());
        assertEquals(0, counts.excluded());
        assertEquals(6, counts.attempted());
        assertEquals(6, counts.acknowledged());
        assertEquals(0, counts.failed());
        assertEquals(counts.lines(), counts.comments() + counts.headers() + counts.rejected() + counts.dataRows());

        assertEquals(2, report.files().size());
        assertTrue(report.files().get(0).path().endsWith("08-11-21.csv"), "day order");
        assertEquals(FIRST_DATA_LINE + 4, report.files().get(0).lines());
        assertTrue(report.files().get(0).complete());
    }

    @Test
    void reportsEachPartitionsOffsetRange() throws IOException {
        MockProducer<byte[], byte[]> producer = producer();
        ReplayReport report = replay(producer, options("--input", week().toString()));

        long[] expected = new long[PARTITIONS];
        for (String symbol : List.of("RDSA.NL", "AIR.FR", "AIR.FR", "846741.ETR", "RDSA.NL", "SIE.ETR")) {
            expected[partitionOf(symbol)]++;
        }
        assertEquals(PARTITIONS, report.offsets().size());
        for (ReplayReport.PartitionRange range : report.offsets()) {
            assertEquals(expected[range.partition()], range.records());
            if (range.records() == 0) {
                assertNull(range.startOffset());
            } else {
                assertEquals(0L, range.startOffset());
                assertEquals(range.records(), range.endOffset());
            }
        }
    }

    @Test
    void replaysOnlyTheSelectedSubset() throws IOException {
        MockProducer<byte[], byte[]> producer = producer();
        ReplayReport report = replay(producer, options("--input", week().toString(), "--exchanges", "FR,ETR"));

        assertEquals(List.of("AIR.FR", "AIR.FR", "846741.ETR", "SIE.ETR"),
                producer.history().stream().map(r -> text(r.key())).toList());
        assertEquals(2, report.counts().excluded());
        assertEquals(4, report.counts().attempted());
        assertEquals(List.of("FR", "ETR"), report.selection().exchanges());

        producer = producer();
        report = replay(producer, options("--input", week().toString(), "--symbols", "RDSA.NL,SIE.ETR", "--limit", "2"));
        assertEquals(List.of("RDSA.NL", "RDSA.NL"), producer.history().stream().map(r -> text(r.key())).toList());
        assertTrue(report.selection().limitReached());
        assertEquals(ReplayReport.Status.COMPLETE, report.status());
        assertFalse(report.files().get(1).complete(), "SIE.ETR in the second file was never read");

        producer = producer();
        report = replay(producer, options("--input", week().toString(), "--limit", "4"));
        assertEquals(2, report.files().size(), "every input is listed, read or not");
        assertTrue(report.files().get(0).complete());
        assertEquals(0, report.files().get(1).lines());
        assertFalse(report.files().get(1).complete());
    }

    @Test
    void stopsAtTheFirstUnacknowledgedRecord() throws IOException {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster("ticks"), true, null,
                new ByteArraySerializer(), new ByteArraySerializer()) {
            private int sends;

            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                if (++sends == 3) {
                    callback.onCompletion(null, new TimeoutException("delivery timed out"));
                    return null;
                }
                return super.send(record, callback);
            }
        };
        ReplayReport report = replay(producer, options("--input", week().toString()));

        assertEquals(ReplayReport.Status.FAILED, report.status());
        assertTrue(report.error().contains("delivery timed out"));
        assertEquals(3, report.counts().attempted());
        assertEquals(2, report.counts().acknowledged());
        assertEquals(1, report.counts().failed());
    }

    @Test
    void countsASendThatThrowsAsFailed() throws IOException {
        MockProducer<byte[], byte[]> producer = producer();
        producer.sendException = new KafkaException("producer closed");
        ReplayReport report = replay(producer, options("--input", week().toString()));

        assertEquals(ReplayReport.Status.FAILED, report.status());
        assertEquals(1, report.counts().attempted());
        assertEquals(1, report.counts().failed());
    }

    @Test
    void anInterruptedReplayFlushesAndSaysSo() throws IOException {
        ReplayOptions options = options("--input", week().toString());
        Replay[] replay = new Replay[1];
        MockProducer<byte[], byte[]> producer = new MockProducer<>(cluster("ticks"), true, null,
                new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                if (history().size() == 1) {
                    replay[0].interrupt();
                }
                return super.send(record, callback);
            }
        };
        replay[0] = new Replay(options, InputFiles.resolve(options.inputs()), producer, PARTITIONS, Map.of(),
                wallMillis::get, nanos::get, nanos::addAndGet);
        ReplayReport report = replay[0].run();

        assertEquals(ReplayReport.Status.INTERRUPTED, report.status());
        assertEquals(2, report.counts().attempted());
        assertEquals(2, report.counts().acknowledged());
    }

    @Test
    void aDryRunAssignsPartitionsWithoutKafka() throws IOException {
        ReplayOptions options = options("--input", week().toString(), "--dry-run", "--partitions", "8");
        Replay replay = new Replay(options, InputFiles.resolve(options.inputs()), null, 8, null,
                wallMillis::get, nanos::get, nanos::addAndGet);
        ReplayReport report = replay.run();

        assertTrue(report.dryRun());
        assertEquals(ReplayReport.Status.COMPLETE, report.status());
        assertEquals(6, report.counts().attempted());
        assertEquals(0, report.counts().acknowledged());
        long[] expected = new long[PARTITIONS];
        for (String symbol : List.of("RDSA.NL", "AIR.FR", "AIR.FR", "846741.ETR", "RDSA.NL", "SIE.ETR")) {
            expected[partitionOf(symbol)]++;
        }
        assertArrayEquals(expected, report.offsets().stream().mapToLong(ReplayReport.PartitionRange::records).toArray());
        assertTrue(report.offsets().stream().allMatch(r -> r.startOffset() == null));
        assertEquals(6 * ("0:14".length() + Replay.SOURCE_HEADER.length()), report.counts().headerBytes());
    }

    @Test
    void pacesARealTimeReplayByTheSourceClock() throws IOException {
        List<String> rows = new ArrayList<>();
        for (int second = 0; second < 600; second++) {
            rows.add(price("AIR.FR", String.format("09:%02d:%02d.000", second / 60, second % 60), "111.940000"));
        }
        TestCsv.write(dir, "debs2022-gc-trading-day-08-11-21.csv", rows.toArray(String[]::new));
        MockProducer<byte[], byte[]> producer = producer();
        long wallBefore = nanos.get();
        ReplayReport report = replay(producer, options("--input", dir.toString(), "--mode", "realtime", "--rate", "60"));

        // The clock trails the rows by one window, so it covers 600 - 256 source seconds.
        double expectedSeconds = (600 - SourceClock.WINDOW) / 60d;
        assertEquals(expectedSeconds, (nanos.get() - wallBefore) / 1e9, 1e-3);
        assertEquals(600, producer.history().size());
        ReplayReport.Pacing pacing = report.pacing();
        assertEquals("2021-11-08T09:00", pacing.clockStart());
        assertEquals("2021-11-08T09:00", pacing.paceStart());
        assertEquals("2021-11-08T09:05:44", pacing.clockEnd());
        assertEquals(60.0, pacing.achievedRate(), 0.01);
        assertEquals(600, pacing.pacedRecords());
        assertEquals(60.0, report.rate());
    }
}
