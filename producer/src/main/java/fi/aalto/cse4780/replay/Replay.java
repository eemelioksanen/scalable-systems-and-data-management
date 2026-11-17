package fi.aalto.cse4780.replay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams the input files into the {@code ticks} topic, one record per data row, and accounts for
 * every line it reads.
 *
 * <p>Each record is keyed by the symbol, carries the 7-field projection as its value (D-13, D-24),
 * as CSV or {@link BinaryTicks} (D-42), and a {@code src} header of {@code <file index>:<line number>}, and is timestamped with the
 * wall-clock time it was handed to the producer, which is the ingest time used for latency (D-15).
 * Rows are sent from one thread in file order, and the idempotent producer keeps that order per
 * partition through retries, so each symbol's Kafka offsets follow its file order (D-23).
 *
 * <p>The first unacknowledged record stops the replay: sending more would leave a gap that no
 * consumer could see, so the run is reported as failed instead.
 */
final class Replay implements Callback {

    private static final Logger LOG = LoggerFactory.getLogger(Replay.class);

    static final String SOURCE_HEADER = "src";
    private static final int READ_BUFFER_CHARS = 1 << 20;
    private static final int MAX_LOGGED_REJECTS = 10;
    private static final String BYTE_ORDER_MARK = "\uFEFF";

    private final ReplayOptions options;
    private final List<Path> files;
    private final Producer<byte[], byte[]> producer;
    private final int partitions;
    private final Map<String, String> producerConfig;
    private final LongSupplier wallClock;
    private final LongSupplier nanoTime;
    private final Pacer pacer;

    private final CsvLine row = new CsvLine();
    private final BinaryTicks binary = new BinaryTicks();
    private final SourceClock clock = new SourceClock();

    private volatile boolean stopRequested;
    private volatile boolean interrupted;
    private volatile Exception failure;

    // Written by the reading thread only.
    private long lines;
    private long comments;
    private long headers;
    private long rejected;
    private long dataRows;
    private long excluded;
    private long filtered;
    private long attempted;
    private long keyBytes;
    private long valueBytes;
    private long headerBytes;
    private long clockStart = SourceClock.UNKNOWN;

    // Written by the producer's I/O thread in onCompletion, or by the reading thread in a dry run.
    private long acknowledged;
    private long failed;
    private final long[] partitionRecords;
    private final long[] partitionFirst;
    private final long[] partitionLast;

    /**
     * @param producer   null for a dry run, which assigns partitions with Kafka's default rule instead
     * @param partitions the topic's partition count
     * @param wallClock  epoch milliseconds, for record timestamps
     * @param sleeper    parks the reading thread for up to the given nanoseconds while pacing
     */
    Replay(ReplayOptions options, List<Path> files, Producer<byte[], byte[]> producer, int partitions,
            Map<String, String> producerConfig, LongSupplier wallClock, LongSupplier nanoTime, LongConsumer sleeper) {
        this.options = options;
        this.files = files;
        this.producer = producer;
        this.partitions = partitions;
        this.producerConfig = producerConfig;
        this.wallClock = wallClock;
        this.nanoTime = nanoTime;
        this.pacer = options.paced()
                ? new Pacer(options.rate(), options.paceFromMillis(), nanoTime, sleeper, () -> stopRequested)
                : null;
        this.partitionRecords = new long[partitions];
        this.partitionFirst = new long[partitions];
        this.partitionLast = new long[partitions];
        Arrays.fill(partitionFirst, Long.MAX_VALUE);
        Arrays.fill(partitionLast, -1);
    }

    /**
     * Stops reading after the current row, for example on SIGTERM. A row waiting for its pacing slot is
     * sent at once. Attempted records are still flushed and accounted for.
     */
    void interrupt() {
        interrupted = true;
        stopRequested = true;
    }

    ReplayReport run() {
        long startedMillis = wallClock.getAsLong();
        long startedNanos = nanoTime.getAsLong();
        long nextProgress = startedNanos + progressNanos();
        List<ReplayReport.InputFile> inputs = new ArrayList<>();

        for (int index = 0; index < files.size(); index++) {
            Path file = files.get(index);
            if (stopping()) {
                inputs.add(new ReplayReport.InputFile(index, file.toString(), size(file), 0, false, null));
                continue;
            }
            LOG.info("Replaying {} ({} of {})", file, index + 1, files.size());
            long fileLines = 0;
            boolean complete = false;
            String md5 = null;
            try (InputStream raw = Files.newInputStream(file)) {
                MessageDigest digest = options.checksum() ? MessageDigest.getInstance("MD5") : null;
                InputStream in = digest == null ? raw : new DigestInputStream(raw, digest);
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), READ_BUFFER_CHARS);
                String line;
                while (!stopping() && (line = reader.readLine()) != null) {
                    fileLines++;
                    lines++;
                    if (fileLines == 1 && line.startsWith(BYTE_ORDER_MARK)) {
                        line = line.substring(1);
                    }
                    switch (row.parse(line)) {
                        case COMMENT -> comments++;
                        case HEADER -> headers++;
                        case REJECTED -> reject(file, fileLines);
                        case DATA -> offer(index, fileLines);
                    }
                    if ((lines & 1023) == 0 && progressNanos() > 0 && nanoTime.getAsLong() >= nextProgress) {
                        logProgress(startedNanos);
                        nextProgress = nanoTime.getAsLong() + progressNanos();
                    }
                }
                complete = !stopping() || reader.readLine() == null;
                if (complete && digest != null) {
                    md5 = HexFormat.of().formatHex(digest.digest());
                }
            } catch (IOException | NoSuchAlgorithmException e) {
                fail(new IOException("reading " + file + " at line " + fileLines + ": " + e.getMessage(), e));
            }
            inputs.add(new ReplayReport.InputFile(index, file.toString(), size(file), fileLines, complete, md5));
        }

        long flushStarted = nanoTime.getAsLong();
        if (producer != null) {
            try {
                producer.flush();
            } catch (KafkaException e) {
                fail(e);
            }
        }
        long finishedNanos = nanoTime.getAsLong();
        return report(inputs, startedMillis, startedNanos, flushStarted, finishedNanos);
    }

    private void offer(int fileIndex, long lineNumber) {
        dataRows++;
        // The clock follows every row, selected or not: it estimates the feed's time, not the subset's.
        long now = SourceClock.UNKNOWN;
        if (pacer != null) {
            now = clock.update(row);
            if (clockStart == SourceClock.UNKNOWN) {
                clockStart = now;
            }
        }
        String symbol = row.field(CsvLine.ID);
        if (!selected(symbol)) {
            excluded++;
            return;
        }
        if (options.earlyFilter() && row.isEmpty(CsvLine.LAST)) {
            filtered++;
            return;
        }
        byte[] value;
        try {
            value = options.encoding() == ReplayOptions.Encoding.CSV
                    ? row.projection().getBytes(StandardCharsets.UTF_8)
                    : binary.encode(row);
        } catch (IllegalArgumentException e) {
            fail(new IllegalArgumentException("file " + fileIndex + " line " + lineNumber + ": " + e.getMessage(), e));
            return;
        }
        if (pacer != null) {
            if (producer != null && pacer.startsPacing(now)) {
                // Drain the unpaced rows first, so the paced part starts with an empty producer buffer.
                try {
                    producer.flush();
                } catch (KafkaException e) {
                    fail(e);
                }
            }
            pacer.await(now);
        }

        byte[] key = symbol.getBytes(StandardCharsets.UTF_8);
        byte[] source = (fileIndex + ":" + lineNumber).getBytes(StandardCharsets.US_ASCII);
        attempted++;
        keyBytes += key.length;
        valueBytes += value.length;
        headerBytes += SOURCE_HEADER.length() + source.length;

        if (producer == null) {
            recordAck(Utils.toPositive(Utils.murmur2(key)) % partitions, -1);
            return;
        }
        Header[] header = {new RecordHeader(SOURCE_HEADER, source)};
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                options.topic(), null, wallClock.getAsLong(), key, value, new RecordHeaders(header));
        try {
            producer.send(record, this);
        } catch (KafkaException | IllegalStateException e) {
            synchronized (this) {
                failed++;
            }
            fail(e);
        }
    }

    private boolean selected(String symbol) {
        if (!options.symbols().isEmpty() && !options.symbols().contains(symbol)) {
            return false;
        }
        if (options.exchanges().isEmpty()) {
            return true;
        }
        int dot = symbol.lastIndexOf('.');
        return dot >= 0 && options.exchanges().contains(symbol.substring(dot + 1));
    }

    private void reject(Path file, long lineNumber) {
        rejected++;
        if (rejected <= MAX_LOGGED_REJECTS) {
            LOG.warn("Rejected {} line {}: not 39 fields or 40 with an empty last one", file, lineNumber);
        }
    }

    /** Producer callback, on the producer's I/O thread. */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            synchronized (this) {
                failed++;
            }
            fail(exception);
            return;
        }
        recordAck(metadata.partition(), metadata.offset());
    }

    private synchronized void recordAck(int partition, long offset) {
        if (offset >= 0) {
            acknowledged++;
            partitionFirst[partition] = Math.min(partitionFirst[partition], offset);
            partitionLast[partition] = Math.max(partitionLast[partition], offset);
        }
        partitionRecords[partition]++;
    }

    private void fail(Exception e) {
        if (failure == null) {
            failure = e;
            LOG.error("Replay failed; stopping after the records already sent: {}", e.toString());
        }
        stopRequested = true;
    }

    private boolean stopping() {
        return stopRequested || attempted >= options.limit();
    }

    private long progressNanos() {
        return options.progressSeconds() * 1_000_000_000L;
    }

    private void logProgress(long startedNanos) {
        double seconds = (nanoTime.getAsLong() - startedNanos) / 1e9;
        long acked;
        synchronized (this) {
            acked = acknowledged;
        }
        if (pacer == null) {
            LOG.info("{} lines, {} records sent ({} rec/s), {} acknowledged",
                    lines, attempted, Math.round(attempted / seconds), acked);
        } else {
            LOG.info("{} lines, {} records sent ({} rec/s), {} acknowledged, source clock {}, lag p99 {} ms",
                    lines, attempted, Math.round(attempted / seconds), acked, sourceTime(clock.now()),
                    pacer.lagQuantileMs(0.99));
        }
    }

    private synchronized ReplayReport report(List<ReplayReport.InputFile> inputs, long startedMillis,
            long startedNanos, long flushStarted, long finishedNanos) {
        ReplayReport.Status status;
        if (failure != null || (producer != null && acknowledged + failed != attempted)) {
            status = ReplayReport.Status.FAILED;
        } else if (interrupted) {
            status = ReplayReport.Status.INTERRUPTED;
        } else {
            status = ReplayReport.Status.COMPLETE;
        }

        List<ReplayReport.PartitionRange> offsets = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            boolean written = partitionLast[p] >= 0;
            offsets.add(new ReplayReport.PartitionRange(p,
                    written ? partitionFirst[p] : null, written ? partitionLast[p] + 1 : null, partitionRecords[p]));
        }

        long durationNanos = Math.max(1, finishedNanos - startedNanos);
        double seconds = durationNanos / 1e9;
        long sent = producer == null ? attempted : acknowledged;
        ReplayReport.Timing timing = new ReplayReport.Timing(
                Instant.ofEpochMilli(startedMillis).toString(),
                Instant.ofEpochMilli(startedMillis + durationNanos / 1_000_000).toString(),
                durationNanos / 1_000_000,
                (finishedNanos - flushStarted) / 1_000_000,
                round(sent / seconds),
                round((keyBytes + valueBytes + headerBytes) / 1e6 / seconds));

        ReplayReport.Pacing pacing = null;
        if (pacer != null) {
            double achieved = pacer.achievedRate();
            pacing = new ReplayReport.Pacing(sourceTime(clockStart), sourceTime(pacer.anchorClock()),
                    sourceTime(clock.now()), pacer.paced(), Double.isNaN(achieved) ? null : round(achieved),
                    pacer.lagQuantileMs(0.5), pacer.lagQuantileMs(0.99), pacer.maxLagMs());
        }

        return new ReplayReport(
                options.runId(),
                status,
                failure == null ? null : failure.toString(),
                options.mode().label(),
                options.paced() ? options.rate() : null,
                options.paceFrom(),
                producer == null,
                producer == null ? null : options.bootstrapServers(),
                options.topic(),
                options.encoding().label(),
                partitions,
                new ReplayReport.Selection(List.copyOf(options.exchanges()), List.copyOf(options.symbols()),
                        options.limit() == Long.MAX_VALUE ? null : options.limit(), attempted >= options.limit(),
                        options.earlyFilter()),
                inputs,
                new ReplayReport.Counts(lines, comments, headers, rejected, dataRows, excluded, filtered, attempted,
                        acknowledged, failed, keyBytes, valueBytes, headerBytes),
                offsets,
                timing,
                pacing,
                producerConfig);
    }

    private static String sourceTime(long millis) {
        return millis == SourceClock.UNKNOWN
                ? null
                : LocalDateTime.ofEpochSecond(Math.floorDiv(millis, 1000), (int) Math.floorMod(millis, 1000) * 1_000_000,
                        ZoneOffset.UTC).toString();
    }

    private static long size(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return -1;
        }
    }

    private static double round(double value) {
        return Math.round(value * 100) / 100d;
    }
}
