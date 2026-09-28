package fi.aalto.cse4780.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.List;
import java.util.Map;

/**
 * What one replay run wrote, and whether it wrote all of it (docs/replayer.md).
 *
 * <p>A consumer treats the run as a finite input only when {@code status} is {@code COMPLETE}. Its
 * records are then exactly the offsets {@code [startOffset, endOffset)} of each partition in
 * {@code offsets}, provided nothing else wrote to the topic during the run, which holds when
 * {@code endOffset - startOffset == records} for every partition.
 *
 * <p>Line accounting: {@code lines = comments + headers + rejected + dataRows},
 * {@code dataRows = excluded + attempted}, and for a completed replay
 * {@code attempted = acknowledged} with {@code failed = 0}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record ReplayReport(
        String runId,
        Status status,
        String error,
        String mode,
        Double rate,
        String paceFrom,
        boolean dryRun,
        String bootstrapServers,
        String topic,
        int partitions,
        Selection selection,
        List<InputFile> files,
        Counts counts,
        List<PartitionRange> offsets,
        Timing timing,
        Pacing pacing,
        Map<String, String> producerConfig) {

    enum Status {
        COMPLETE,
        /** A record was not acknowledged, or an input could not be read. The topic holds a partial replay. */
        FAILED,
        /** Stopped by a signal. Everything attempted was flushed and is accounted for. */
        INTERRUPTED
    }

    /** Which rows were replayed. Empty lists mean all. */
    record Selection(List<String> exchanges, List<String> symbols, Long limit, boolean limitReached) {}

    /** {@code index} is the file part of each record's {@code src} header. {@code md5} is set with {@code --checksum}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record InputFile(int index, String path, long bytes, long lines, boolean complete, String md5) {}

    record Counts(
            long lines,
            long comments,
            long headers,
            long rejected,
            long dataRows,
            long excluded,
            long attempted,
            long acknowledged,
            long failed,
            long keyBytes,
            long valueBytes,
            long headerBytes) {}

    /** Offsets are null for a partition that received nothing, and in a dry run. */
    record PartitionRange(int partition, Long startOffset, Long endOffset, long records) {}

    record Timing(
            String startedAt,
            String finishedAt,
            long durationMs,
            long flushMs,
            double recordsPerSecond,
            double payloadMegabytesPerSecond) {}

    /**
     * Real-time pacing: the source clock (local times) where the input started, where pacing began,
     * and where it ended; the rate actually achieved over the paced part; and how late paced rows
     * were sent against their schedule.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Pacing(
            String clockStart,
            String paceStart,
            String clockEnd,
            long pacedRecords,
            Double achievedRate,
            long lagP50Ms,
            long lagP99Ms,
            long lagMaxMs) {}

    static final ObjectMapper JSON = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
}
