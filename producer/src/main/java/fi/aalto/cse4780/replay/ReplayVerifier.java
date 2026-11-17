package fi.aalto.cse4780.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks a replay against what Kafka holds and, with {@code --source}, against its input files.
 *
 * <pre>
 * java -cp producer.jar fi.aalto.cse4780.replay.ReplayVerifier --report PATH|- [--source]
 *     [--bootstrap-servers HOST:PORT] [--output PATH]
 * </pre>
 *
 * <p>The Kafka pass reads exactly the report's offset ranges and checks that they hold the reported
 * record counts without gaps, that every record sits in the partition Kafka's default partitioner
 * gives its key and each symbol in only one partition, that the value has 7 fields led by the key,
 * that each symbol's {@code src} positions increase (file order, D-23), and that timestamps are
 * create times within the run.
 *
 * <p>The source pass re-reads the CSV files without the replayer's code: it locates the 7 columns
 * by header name, splits rows naively, and applies the report's selection, including its early
 * filter. Only CSV values can be verified; a binary replay (D-42) is reported as unverifiable. Equal per-partition
 * {@link ContentDigest}s then show that every field and the per-partition order were preserved.
 * {@code contentDigest} does not depend on offsets or timestamps, so it is identical for replays of
 * the same rows at different speeds.
 *
 * <p>Prints a JSON verdict and exits 0 only if every check passed.
 */
public final class ReplayVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayVerifier.class);

    private static final List<String> PROJECTED_COLUMNS =
            List.of("ID", "SecType", "Date", "Time", "Last", "Trading time", "Trading date");
    private static final int LAST_COLUMN = PROJECTED_COLUMNS.indexOf("Last");
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);
    private static final long TIMESTAMP_SLACK_MS = 1_000;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Verification(
            String runId,
            String topic,
            boolean passed,
            List<String> problems,
            String contentDigest,
            KafkaSide kafka,
            SourceSide source) {}

    record PartitionDigest(int partition, long records, String sha256) {}

    record Checks(
            long offsetGaps,
            long wrongPartition,
            long symbolsInSeveralPartitions,
            long keyNotFirstField,
            long notSevenFields,
            long missingSourceHeader,
            long sourceOrderViolations,
            long notCreateTime,
            long timestampOutsideRun) {

        boolean clean() {
            return offsetGaps + wrongPartition + symbolsInSeveralPartitions + keyNotFirstField + notSevenFields
                    + missingSourceHeader + sourceOrderViolations + notCreateTime + timestampOutsideRun == 0;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record KafkaSide(
            long records,
            long symbols,
            List<PartitionDigest> partitions,
            Checks checks,
            String firstTimestamp,
            String lastTimestamp,
            Double sourcePerWallRate) {}

    record SourceSide(long rows, long selected, long rejected, List<PartitionDigest> partitions) {}

    private ReplayVerifier() {}

    public static void main(String[] args) throws Exception {
        String reportPath = null;
        String bootstrap = null;
        boolean source = false;
        Path output = null;
        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            if (option.equals("--source")) {
                source = true;
                continue;
            }
            if (i + 1 >= args.length) {
                usage();
            }
            String value = args[++i];
            switch (option) {
                case "--report" -> reportPath = value;
                case "--bootstrap-servers" -> bootstrap = value;
                case "--output" -> output = Path.of(value);
                default -> usage();
            }
        }
        if (reportPath == null) {
            usage();
        }
        ReplayReport report;
        try (InputStream in = reportPath.equals("-") ? System.in : Files.newInputStream(Path.of(reportPath))) {
            report = ReplayReport.JSON.readValue(in, ReplayReport.class);
        }
        if (bootstrap == null) {
            bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", report.bootstrapServers());
        }

        Verification verification = verify(report, bootstrap, source);
        String json = ReplayReport.JSON.writeValueAsString(verification);
        System.out.println(json);
        if (output != null) {
            Files.writeString(output, json + System.lineSeparator());
        }
        System.exit(verification.passed() ? 0 : 1);
    }

    private static void usage() {
        System.err.println("usage: ReplayVerifier --report PATH|- [--source] [--bootstrap-servers HOST:PORT] "
                + "[--output PATH]");
        System.exit(2);
    }

    static Verification verify(ReplayReport report, String bootstrap, boolean withSource) throws IOException {
        List<String> problems = new ArrayList<>();
        if (report.encoding() != null && !report.encoding().equals(ReplayOptions.Encoding.CSV.label())) {
            problems.add("only CSV values can be verified, and this replay wrote " + report.encoding() + " ones");
            return new Verification(report.runId(), report.topic(), false, problems, null, null, null);
        }
        if (report.status() != ReplayReport.Status.COMPLETE) {
            problems.add("the replay did not complete: " + report.status());
        }
        if (report.dryRun()) {
            problems.add("a dry run wrote nothing to Kafka");
        }
        for (ReplayReport.PartitionRange range : report.offsets()) {
            if (range.startOffset() != null && range.endOffset() - range.startOffset() != range.records()) {
                problems.add("partition " + range.partition() + ": offsets " + range.startOffset() + ".."
                        + range.endOffset() + " hold more than the " + range.records()
                        + " records this run wrote; another writer shared the topic");
            }
        }

        KafkaSide kafka = readKafka(report, bootstrap, problems);
        SourceSide source = withSource ? readSource(report) : null;

        if (kafka.records() != report.counts().acknowledged()) {
            problems.add("Kafka holds " + kafka.records() + " records in the run's ranges, the report acknowledged "
                    + report.counts().acknowledged());
        }
        if (!kafka.checks().clean()) {
            problems.add("record checks failed: " + kafka.checks());
        }
        if (source != null) {
            if (source.selected() != report.counts().attempted()) {
                problems.add("the source selects " + source.selected() + " rows, the report attempted "
                        + report.counts().attempted());
            }
            for (int p = 0; p < report.partitions(); p++) {
                PartitionDigest fromKafka = kafka.partitions().get(p);
                PartitionDigest fromSource = source.partitions().get(p);
                if (!fromKafka.equals(fromSource)) {
                    problems.add("partition " + p + ": Kafka " + fromKafka + " but source " + fromSource);
                }
            }
        }
        return new Verification(report.runId(), report.topic(), problems.isEmpty(), problems,
                overallDigest(kafka.partitions()), kafka, source);
    }

    private static KafkaSide readKafka(ReplayReport report, String bootstrap, List<String> problems) {
        int partitions = report.partitions();
        ContentDigest[] digests = new ContentDigest[partitions];
        long[] expectedOffset = new long[partitions];
        Map<TopicPartition, Long> ends = new HashMap<>();
        for (ReplayReport.PartitionRange range : report.offsets()) {
            digests[range.partition()] = new ContentDigest();
            if (range.startOffset() != null) {
                ends.put(new TopicPartition(report.topic(), range.partition()), range.endOffset());
                expectedOffset[range.partition()] = range.startOffset();
            }
        }

        Map<String, Integer> symbolPartition = new HashMap<>();
        Map<String, long[]> lastSource = new HashMap<>();
        long offsetGaps = 0, wrongPartition = 0, splitSymbols = 0, keyNotFirst = 0, notSeven = 0;
        long missingSource = 0, orderViolations = 0, notCreateTime = 0, outsideRun = 0;
        long minTs = Long.MAX_VALUE, maxTs = Long.MIN_VALUE;
        long runStart = Instant.parse(report.timing().startedAt()).toEpochMilli() - TIMESTAMP_SLACK_MS;
        long runEnd = Instant.parse(report.timing().finishedAt()).toEpochMilli() + TIMESTAMP_SLACK_MS;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10_000);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(ends.keySet());
            ends.keySet().forEach(tp -> consumer.seek(tp, expectedOffset[tp.partition()]));
            Set<TopicPartition> remaining = new HashSet<>(ends.keySet());
            remaining.removeIf(tp -> expectedOffset[tp.partition()] >= ends.get(tp));
            long lastData = System.nanoTime();
            long read = 0;
            while (!remaining.isEmpty()) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
                if (batch.isEmpty()) {
                    if (System.nanoTime() - lastData > IDLE_TIMEOUT.toNanos()) {
                        problems.add("no data for " + IDLE_TIMEOUT.toSeconds() + " s before reaching the end of "
                                + remaining);
                        break;
                    }
                    continue;
                }
                lastData = System.nanoTime();
                for (TopicPartition tp : batch.partitions()) {
                    int p = tp.partition();
                    long end = ends.get(tp);
                    for (ConsumerRecord<byte[], byte[]> record : batch.records(tp)) {
                        if (record.offset() >= end) {
                            break;
                        }
                        if (record.offset() != expectedOffset[p]) {
                            offsetGaps++;
                        }
                        expectedOffset[p] = record.offset() + 1;

                        byte[] key = record.key();
                        byte[] value = record.value() == null ? new byte[0] : record.value();
                        Header header = record.headers().lastHeader(Replay.SOURCE_HEADER);
                        byte[] src = header == null ? null : header.value();
                        digests[p].add(key, value, src);
                        read++;

                        String symbol = key == null ? "" : new String(key, StandardCharsets.UTF_8);
                        if (key == null || Utils.toPositive(Utils.murmur2(key)) % partitions != p) {
                            wrongPartition++;
                        }
                        Integer previous = symbolPartition.putIfAbsent(symbol, p);
                        if (previous != null && previous != p) {
                            splitSymbols++;
                        }
                        String text = new String(value, StandardCharsets.UTF_8);
                        if (!text.startsWith(symbol + ",")) {
                            keyNotFirst++;
                        }
                        if (text.split(",", -1).length != PROJECTED_COLUMNS.size()) {
                            notSeven++;
                        }
                        if (src == null) {
                            missingSource++;
                        } else {
                            long[] position = parseSource(src);
                            long[] last = lastSource.put(symbol, position);
                            if (position == null || (last != null && compare(last, position) >= 0)) {
                                orderViolations++;
                            }
                        }
                        if (record.timestampType() != TimestampType.CREATE_TIME) {
                            notCreateTime++;
                        }
                        long ts = record.timestamp();
                        if (ts < runStart || ts > runEnd) {
                            outsideRun++;
                        }
                        minTs = Math.min(minTs, ts);
                        maxTs = Math.max(maxTs, ts);
                    }
                    if (expectedOffset[p] >= end) {
                        remaining.remove(tp);
                        consumer.pause(List.of(tp));
                    }
                }
                if (read % 5_000_000 < batch.count()) {
                    LOG.info("Read {} records", read);
                }
            }
        }

        List<PartitionDigest> partitionDigests = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            partitionDigests.add(new PartitionDigest(p, digests[p].records(), digests[p].hex()));
        }
        long total = partitionDigests.stream().mapToLong(PartitionDigest::records).sum();
        return new KafkaSide(total, symbolPartition.size(), partitionDigests,
                new Checks(offsetGaps, wrongPartition, splitSymbols, keyNotFirst, notSeven, missingSource,
                        orderViolations, notCreateTime, outsideRun),
                total == 0 ? null : Instant.ofEpochMilli(minTs).toString(),
                total == 0 ? null : Instant.ofEpochMilli(maxTs).toString(),
                sourcePerWallRate(report, minTs, maxTs));
    }

    /**
     * For an unanchored real-time replay, the source time the run covered per wall-clock time between
     * its first and last record timestamps.
     */
    private static Double sourcePerWallRate(ReplayReport report, long minTs, long maxTs) {
        ReplayReport.Pacing pacing = report.pacing();
        if (pacing == null || report.paceFrom() != null || pacing.clockStart() == null || maxTs <= minTs) {
            return null;
        }
        long sourceSpan = LocalDateTime.parse(pacing.clockEnd()).toInstant(ZoneOffset.UTC).toEpochMilli()
                - LocalDateTime.parse(pacing.clockStart()).toInstant(ZoneOffset.UTC).toEpochMilli();
        return Math.round(sourceSpan * 100d / (maxTs - minTs)) / 100d;
    }

    /** Re-reads the input files independently of {@link CsvLine} and {@link Replay}. */
    static SourceSide readSource(ReplayReport report) throws IOException {
        int partitions = report.partitions();
        ContentDigest[] digests = new ContentDigest[partitions];
        Arrays.setAll(digests, p -> new ContentDigest());
        Set<String> exchanges = new HashSet<>(report.selection().exchanges());
        Set<String> symbols = new HashSet<>(report.selection().symbols());
        long limit = report.selection().limit() == null ? Long.MAX_VALUE : report.selection().limit();
        long rows = 0, selected = 0, rejected = 0;

        files:
        for (ReplayReport.InputFile file : report.files()) {
            LOG.info("Re-reading {}", file.path());
            int[] columns = null;
            try (BufferedReader reader = Files.newBufferedReader(Path.of(file.path()), StandardCharsets.UTF_8)) {
                String line;
                long lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (lineNumber == 1 && !line.isEmpty() && line.charAt(0) == '\uFEFF') {
                        line = line.substring(1);
                    }
                    if (line.startsWith("#")) {
                        continue;
                    }
                    String[] fields = line.split(",", -1);
                    if (columns == null && fields[0].trim().equals("ID")) {
                        columns = columnsByName(fields, file.path());
                        continue;
                    }
                    if (columns == null) {
                        throw new IOException(file.path() + " line " + lineNumber + ": data before the header row");
                    }
                    if (fields[0].trim().equals("ID")) {
                        continue;
                    }
                    rows++;
                    if (fields.length == 40 && fields[39].isEmpty()) {
                        fields = Arrays.copyOf(fields, 39);
                    }
                    if (fields.length != 39) {
                        rejected++;
                        continue;
                    }
                    String symbol = fields[columns[0]];
                    int dot = symbol.lastIndexOf('.');
                    String exchange = dot < 0 ? null : symbol.substring(dot + 1);
                    if ((!symbols.isEmpty() && !symbols.contains(symbol))
                            || (!exchanges.isEmpty() && !exchanges.contains(exchange))) {
                        continue;
                    }
                    if (report.selection().earlyFilter() && fields[columns[LAST_COLUMN]].isEmpty()) {
                        continue;
                    }
                    if (selected == limit) {
                        break files;
                    }
                    selected++;
                    StringBuilder value = new StringBuilder();
                    for (int i = 0; i < columns.length; i++) {
                        value.append(i == 0 ? "" : ",").append(fields[columns[i]]);
                    }
                    byte[] key = symbol.getBytes(StandardCharsets.UTF_8);
                    int partition = Utils.toPositive(Utils.murmur2(key)) % partitions;
                    digests[partition].add(key, value.toString().getBytes(StandardCharsets.UTF_8),
                            (file.index() + ":" + lineNumber).getBytes(StandardCharsets.US_ASCII));
                }
            }
        }

        List<PartitionDigest> partitionDigests = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            partitionDigests.add(new PartitionDigest(p, digests[p].records(), digests[p].hex()));
        }
        return new SourceSide(rows, selected, rejected, partitionDigests);
    }

    private static int[] columnsByName(String[] header, String file) throws IOException {
        List<String> names = Arrays.stream(header).map(String::trim).toList();
        int[] columns = new int[PROJECTED_COLUMNS.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = names.indexOf(PROJECTED_COLUMNS.get(i));
            if (columns[i] < 0) {
                throw new IOException(file + ": no column named " + PROJECTED_COLUMNS.get(i));
            }
        }
        return columns;
    }

    /** {@code <file index>:<line>}, or null. */
    private static long[] parseSource(byte[] src) {
        String text = new String(src, StandardCharsets.US_ASCII);
        int colon = text.indexOf(':');
        try {
            return new long[] {Long.parseLong(text.substring(0, colon)), Long.parseLong(text.substring(colon + 1))};
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static int compare(long[] a, long[] b) {
        return a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]);
    }

    /** One digest for the whole replay: partition records and digests, in partition order. */
    static String overallDigest(List<PartitionDigest> partitions) {
        ContentDigest digest = new ContentDigest();
        for (PartitionDigest p : partitions) {
            digest.add(Integer.toString(p.partition()).getBytes(StandardCharsets.US_ASCII),
                    Long.toString(p.records()).getBytes(StandardCharsets.US_ASCII),
                    p.sha256().getBytes(StandardCharsets.US_ASCII));
        }
        return digest.hex();
    }
}
