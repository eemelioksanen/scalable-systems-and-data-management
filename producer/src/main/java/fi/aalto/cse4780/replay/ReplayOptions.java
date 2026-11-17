package fi.aalto.cse4780.replay;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Command-line options of {@link Replayer}. */
record ReplayOptions(
        List<Path> inputs,
        Set<String> exchanges,
        Set<String> symbols,
        long limit,
        boolean earlyFilter,
        Encoding encoding,
        Mode mode,
        double rate,
        String paceFrom,
        String bootstrapServers,
        String topic,
        Map<String, String> producerProperties,
        boolean dryRun,
        int dryRunPartitions,
        Path report,
        String runId,
        boolean checksum,
        int progressSeconds) {

    /** The {@code ticks} value format (D-14, D-42). */
    enum Encoding {
        /** The 7-field CSV projection. */
        CSV,
        /** {@link BinaryTicks}. */
        BINARY;

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    enum Mode {
        /** Write the input into Kafka before the job runs, as fast as possible (throughput runs). */
        PRELOAD,
        /** Replay as fast as possible while the job runs. */
        MAX,
        /** Pace by the source clock, {@code rate} times faster than real time (latency runs). */
        REALTIME;

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    static final String USAGE = """
            Replays DEBS 2022 trading-data CSV files into the Kafka ticks topic (docs/replayer.md).

              java -cp producer.jar fi.aalto.cse4780.replay.Replayer --input PATH [options]

            Input
              --input PATH                  CSV file, or a directory of day files replayed in day order; repeatable
              --exchanges FR,NL             only these exchanges (the ID suffix)
              --symbols RDSA.NL,AIR.FR      only these symbols
              --symbols-file PATH           only the symbols listed in the file, one per line
              --limit N                     stop after N records
              --early-filter                send only rows with a Last (price rows), counting the rest as filtered

            Encoding
              --encoding csv|binary         ticks value format (default csv); the job's --ticks-encoding must match

            Mode
              --mode preload|max|realtime   default preload
              --rate K                      realtime: K times faster than real time (default 1)
              --pace-from 2021-11-08T09:00  realtime: send rows before this source time unpaced

            Kafka
              --bootstrap-servers HOST:PORT default $BOOTSTRAP_SERVERS, else localhost:29092
              --topic NAME                  default ticks
              --producer-property KEY=VALUE override a producer setting; repeatable
              --dry-run                     read and project only, assigning partitions without Kafka
              --partitions N                partition count for --dry-run (default 8)

            Report (JSON on stdout; progress and errors on stderr)
              --report PATH                 also write the report to this file
              --run-id ID                   default replay-<UTC start time>
              --checksum                    record each input file's MD5
              --progress-seconds N          progress log interval (default 10, 0 = off)

            Exit status: 0 when the replay completed, 1 when it failed or was interrupted, 2 for usage errors.
            """;

    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    boolean paced() {
        return mode == Mode.REALTIME;
    }

    /** {@link #paceFrom} as source-clock milliseconds, or {@link SourceClock#UNKNOWN}. */
    long paceFromMillis() {
        return paceFrom == null
                ? SourceClock.UNKNOWN
                : LocalDateTime.parse(paceFrom).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /** Parses {@code args}. Throws {@link IllegalArgumentException} with a message for the user. */
    static ReplayOptions parse(String[] args, Map<String, String> env, Instant now) throws IOException {
        List<Path> inputs = new ArrayList<>();
        Set<String> exchanges = new LinkedHashSet<>();
        Set<String> symbols = new LinkedHashSet<>();
        long limit = Long.MAX_VALUE;
        boolean earlyFilter = false;
        Encoding encoding = Encoding.CSV;
        Mode mode = Mode.PRELOAD;
        Double rate = null;
        String paceFrom = null;
        String bootstrap = env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:29092");
        String topic = "ticks";
        Map<String, String> producerProperties = new LinkedHashMap<>();
        boolean dryRun = false;
        Integer partitions = null;
        Path report = null;
        String runId = "replay-" + RUN_ID_TIME.format(now);
        boolean checksum = false;
        int progressSeconds = 10;

        for (int i = 0; i < args.length; i++) {
            String option = args[i];
            switch (option) {
                case "--dry-run" -> dryRun = true;
                case "--checksum" -> checksum = true;
                case "--early-filter" -> earlyFilter = true;
                default -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("unknown option or missing value: " + option);
                    }
                    String value = args[++i];
                    switch (option) {
                        case "--input" -> inputs.add(Path.of(value));
                        case "--exchanges" -> exchanges.addAll(list(value));
                        case "--symbols" -> symbols.addAll(list(value));
                        case "--symbols-file" -> symbols.addAll(symbolsFile(Path.of(value)));
                        case "--limit" -> limit = positiveLong(option, value);
                        case "--mode" -> mode = mode(value);
                        case "--encoding" -> encoding = encoding(value);
                        case "--rate" -> rate = positiveDouble(option, value);
                        case "--pace-from" -> paceFrom = sourceTime(value);
                        case "--bootstrap-servers" -> bootstrap = value;
                        case "--topic" -> topic = value;
                        case "--producer-property" -> {
                            int eq = value.indexOf('=');
                            if (eq <= 0) {
                                throw new IllegalArgumentException("--producer-property needs KEY=VALUE: " + value);
                            }
                            producerProperties.put(value.substring(0, eq), value.substring(eq + 1));
                        }
                        case "--partitions" -> partitions = (int) positiveLong(option, value);
                        case "--report" -> report = Path.of(value);
                        case "--run-id" -> runId = value;
                        case "--progress-seconds" -> progressSeconds = (int) nonNegativeLong(option, value);
                        default -> throw new IllegalArgumentException("unknown option: " + option);
                    }
                }
            }
        }

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("--input is required");
        }
        if (mode != Mode.REALTIME && (rate != null || paceFrom != null)) {
            throw new IllegalArgumentException("--rate and --pace-from apply only to --mode realtime");
        }
        if (!dryRun && partitions != null) {
            throw new IllegalArgumentException("--partitions applies only to --dry-run; a replay uses the topic's");
        }
        return new ReplayOptions(inputs, exchanges, symbols, limit, earlyFilter, encoding, mode,
                rate == null ? 1 : rate, paceFrom,
                bootstrap, topic, producerProperties, dryRun, partitions == null ? 8 : partitions, report, runId,
                checksum, progressSeconds);
    }

    private static List<String> list(String value) {
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static List<String> symbolsFile(Path file) throws IOException {
        return Files.readAllLines(file).stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty() && !s.startsWith("#"))
                .toList();
    }

    private static Mode mode(String value) {
        try {
            return Mode.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("--mode must be preload, max or realtime: " + value);
        }
    }

    private static Encoding encoding(String value) {
        try {
            return Encoding.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("--encoding must be csv or binary: " + value);
        }
    }

    private static String sourceTime(String value) {
        try {
            return LocalDateTime.parse(value).toString();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("--pace-from needs a local date-time such as 2021-11-08T09:00: " + value);
        }
    }

    private static long positiveLong(String option, String value) {
        long parsed = nonNegativeLong(option, value);
        if (parsed == 0) {
            throw new IllegalArgumentException(option + " must be positive: " + value);
        }
        return parsed;
    }

    private static long nonNegativeLong(String option, String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " needs a non-negative integer: " + value);
        }
    }

    private static double positiveDouble(String option, String value) {
        try {
            double parsed = Double.parseDouble(value);
            if (!(parsed > 0) || Double.isInfinite(parsed)) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " needs a positive number: " + value);
        }
    }
}
