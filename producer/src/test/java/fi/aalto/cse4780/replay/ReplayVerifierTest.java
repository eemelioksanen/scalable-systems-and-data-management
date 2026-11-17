package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.ReplayTest.PARTITIONS;
import static fi.aalto.cse4780.replay.ReplayTest.partitionOf;
import static fi.aalto.cse4780.replay.ReplayTest.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplayVerifierTest {

    @TempDir
    Path dir;

    /** Per-partition digests of what the replayer sent, as the Kafka pass would compute them. */
    private static List<ReplayVerifier.PartitionDigest> digests(List<ProducerRecord<byte[], byte[]>> sent) {
        ContentDigest[] digests = new ContentDigest[PARTITIONS];
        Arrays.setAll(digests, p -> new ContentDigest());
        for (ProducerRecord<byte[], byte[]> record : sent) {
            digests[partitionOf(text(record.key()))].add(record.key(), record.value(),
                    record.headers().lastHeader(Replay.SOURCE_HEADER).value());
        }
        List<ReplayVerifier.PartitionDigest> result = new ArrayList<>();
        for (int p = 0; p < PARTITIONS; p++) {
            result.add(new ReplayVerifier.PartitionDigest(p, digests[p].records(), digests[p].hex()));
        }
        return result;
    }

    private ReplayReport replay(MockProducer<byte[], byte[]> producer, String... args) throws IOException {
        ReplayOptions options = ReplayTest.options(args);
        return new Replay(options, InputFiles.resolve(options.inputs()), producer, PARTITIONS, Map.of(),
                System::currentTimeMillis, System::nanoTime, n -> {}).run();
    }

    @Test
    void anIndependentReReadOfTheSourceMatchesWhatWasSent() throws IOException {
        Path week = TestCsv.week(dir);
        MockProducer<byte[], byte[]> producer = ReplayTest.producer();
        ReplayReport report = replay(producer, "--input", week.toString());

        ReplayVerifier.SourceSide source = ReplayVerifier.readSource(report);

        assertEquals(digests(producer.history()), source.partitions());
        assertEquals(7, source.rows());
        assertEquals(6, source.selected());
        assertEquals(1, source.rejected());
    }

    @Test
    void theReReadAppliesTheSameSelectionAndLimit() throws IOException {
        Path week = TestCsv.week(dir);
        MockProducer<byte[], byte[]> producer = ReplayTest.producer();
        ReplayReport report = replay(producer, "--input", week.toString(), "--exchanges", "NL,ETR", "--limit", "3");

        ReplayVerifier.SourceSide source = ReplayVerifier.readSource(report);

        assertEquals(3, producer.history().size());
        assertEquals(digests(producer.history()), source.partitions());
    }

    @Test
    void theReReadAppliesTheEarlyFilter() throws IOException {
        Path week = TestCsv.week(dir);
        MockProducer<byte[], byte[]> producer = ReplayTest.producer();
        ReplayReport report = replay(producer, "--input", week.toString(), "--early-filter");

        ReplayVerifier.SourceSide source = ReplayVerifier.readSource(report);

        assertEquals(4, producer.history().size());
        assertEquals(4, source.selected());
        assertEquals(digests(producer.history()), source.partitions());
    }

    @Test
    void aBinaryReplayIsReportedAsUnverifiable() throws IOException {
        Path week = TestCsv.week(dir);
        ReplayReport report = replay(ReplayTest.producer(), "--input", week.toString(), "--encoding", "binary");

        ReplayVerifier.Verification verification = ReplayVerifier.verify(report, "unused:9092", true);

        assertFalse(verification.passed());
        assertEquals(List.of("only CSV values can be verified, and this replay wrote binary ones"),
                verification.problems());
    }

    @Test
    void aChangedFieldChangesTheDigest() throws IOException {
        Path week = TestCsv.week(dir);
        MockProducer<byte[], byte[]> producer = ReplayTest.producer();
        ReplayReport report = replay(producer, "--input", week.toString());

        List<ProducerRecord<byte[], byte[]>> altered = new ArrayList<>(producer.history());
        ProducerRecord<byte[], byte[]> first = altered.get(0);
        altered.set(0, new ProducerRecord<>(first.topic(), null, first.timestamp(), first.key(),
                text(first.value()).replace("19.280000", "19.28").getBytes(StandardCharsets.UTF_8),
                first.headers()));

        assertNotEquals(digests(altered), ReplayVerifier.readSource(report).partitions());
    }

    @Test
    void theReportSurvivesAJsonRoundTrip() throws IOException {
        Path week = TestCsv.week(dir);
        ReplayReport report = replay(ReplayTest.producer(), "--input", week.toString(), "--symbols", "AIR.FR");

        String json = ReplayReport.JSON.writeValueAsString(report);
        assertEquals(report, ReplayReport.JSON.readValue(json, ReplayReport.class));
    }
}
