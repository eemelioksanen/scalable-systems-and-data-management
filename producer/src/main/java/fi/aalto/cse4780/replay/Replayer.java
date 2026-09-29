package fi.aalto.cse4780.replay;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays DEBS 2022 trading-data CSV files into the {@code ticks} topic. See {@link ReplayOptions#USAGE}
 * and docs/replayer.md.
 *
 * <p>The JSON {@link ReplayReport} goes to stdout, and to {@code --report} if given; logs go to stderr.
 */
public final class Replayer {

    private static final Logger LOG = LoggerFactory.getLogger(Replayer.class);

    /** How long a signal waits for the replay to flush and report before the JVM exits anyway. */
    private static final long SHUTDOWN_GRACE_MILLIS = 60_000;

    private static Thread shutdownHook;

    private Replayer() {}

    public static void main(String[] args) throws Exception {
        if (Arrays.asList(args).contains("--help") || Arrays.asList(args).contains("-h")) {
            System.out.print(ReplayOptions.USAGE);
            return;
        }
        ReplayOptions options;
        List<Path> files;
        try {
            options = ReplayOptions.parse(args, System.getenv(), Instant.now());
            files = InputFiles.resolve(options.inputs());
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            System.err.print(ReplayOptions.USAGE);
            System.exit(2);
            return;
        }

        Map<String, String> config = producerConfig(options);
        ReplayReport report;
        if (options.dryRun()) {
            report = run(options, files, null, options.dryRunPartitions(), null);
        } else {
            Map<String, Object> properties = new LinkedHashMap<>(config);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
                int partitions;
                try {
                    partitions = producer.partitionsFor(options.topic()).size();
                } catch (KafkaException e) {
                    LOG.error("Topic {} is not available at {}: {}", options.topic(), options.bootstrapServers(),
                            e.toString());
                    System.exit(1);
                    return;
                }
                report = run(options, files, producer, partitions, config);
            }
        }

        String json = ReplayReport.JSON.writeValueAsString(report);
        System.out.println(json);
        if (options.report() != null) {
            Path parent = options.report().toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Files.writeString(options.report(), json + System.lineSeparator());
        }
        LOG.info("Replay {} {}: {} of {} records acknowledged in {} ms", report.runId(), report.status(),
                report.counts().acknowledged(), report.counts().attempted(), report.timing().durationMs());
        exit(report.status() == ReplayReport.Status.COMPLETE ? 0 : 1);
    }

    private static ReplayReport run(ReplayOptions options, List<Path> files, KafkaProducer<byte[], byte[]> producer,
            int partitions, Map<String, String> config) {
        Replay replay = new Replay(options, files, producer, partitions, config, System::currentTimeMillis,
                System::nanoTime, LockSupport::parkNanos);
        Thread main = Thread.currentThread();
        shutdownHook = new Thread(() -> {
            LOG.warn("Stopping: flushing the records already sent");
            replay.interrupt();
            try {
                main.join(SHUTDOWN_GRACE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "replay-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        return replay.run();
    }

    /** Exits unless a signal is already shutting the JVM down, in which case returning lets it finish. */
    private static void exit(int status) {
        try {
            if (shutdownHook != null) {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
        } catch (IllegalStateException shuttingDown) {
            return;
        }
        System.exit(status);
    }

    /**
     * Producer settings (D-31): an idempotent producer with {@code acks=all}, which keeps each
     * partition in send order through retries (D-15), and lz4 compression, which stores a day in a
     * quarter of the space for under 10% of preload throughput. A preload uses large batches, since
     * nothing reads the topic until it finishes. Live modes keep Kafka's batching defaults so
     * batching adds little latency. {@code --producer-property} overrides any of these.
     */
    static Map<String, String> producerConfig(ReplayOptions options) {
        Map<String, String> config = new LinkedHashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, options.runId());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        if (options.mode() == ReplayOptions.Mode.PRELOAD) {
            config.put(ProducerConfig.LINGER_MS_CONFIG, "50");
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(1 << 20));
        }
        config.putAll(options.producerProperties());
        return config;
    }
}
