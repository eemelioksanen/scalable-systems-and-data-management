package fi.aalto.cse4780.viz;

import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.util.Map;

/** Command-line options of {@link Dashboard}. */
record DashboardOptions(
        String bootstrapServers,
        int port,
        ZoneOffset zoneOffset,
        String resultsTopic,
        String advisoriesTopic,
        String subscriptionsTopic,
        String ticksTopic,
        long flushMs) {

    static final String USAGE = """
            Serves the dashboard over ema-results and advisories (viz/README.md).

              java -jar viz.jar [options]

              --bootstrap-servers HOST:PORT  Kafka (default: $BOOTSTRAP_SERVERS, else localhost:29092)
              --port N                       HTTP and WebSocket port (default 8088)
              --zone-offset +02:00           the Flink job's --zone-offset, used to label times (default +02:00)
              --results-topic NAME           default ema-results
              --advisories-topic NAME        default advisories
              --subscriptions-topic NAME     the traders' lists, for the subscription filter (default subscriptions)
              --ticks-topic NAME             read for the replay clock (default ticks)
              --flush-ms N                   how often browsers get updates (default 250)
            """;

    static DashboardOptions parse(String[] args, Map<String, String> env) {
        String bootstrap = env.getOrDefault("BOOTSTRAP_SERVERS", "localhost:29092");
        int port = 8088;
        ZoneOffset zone = ZoneOffset.of("+02:00");
        String results = "ema-results";
        String advisories = "advisories";
        String subscriptions = "subscriptions";
        String ticks = "ticks";
        long flushMs = 250;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (flag.equals("--help") || flag.equals("-h")) {
                throw new IllegalArgumentException(USAGE);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for " + flag + "\n\n" + USAGE);
            }
            String value = args[++i];
            try {
                switch (flag) {
                    case "--bootstrap-servers" -> bootstrap = value;
                    case "--port" -> port = Integer.parseInt(value);
                    case "--zone-offset" -> zone = ZoneOffset.of(value);
                    case "--results-topic" -> results = value;
                    case "--advisories-topic" -> advisories = value;
                    case "--subscriptions-topic" -> subscriptions = value;
                    case "--ticks-topic" -> ticks = value;
                    case "--flush-ms" -> flushMs = Long.parseLong(value);
                    default -> throw new IllegalArgumentException("Unknown option " + flag + "\n\n" + USAGE);
                }
            } catch (NumberFormatException | DateTimeException e) {
                throw new IllegalArgumentException("Invalid value for " + flag + ": " + value);
            }
        }
        if (port < 0 || port > 65535 || flushMs <= 0) {
            throw new IllegalArgumentException("--port must be 0-65535 and --flush-ms positive");
        }
        return new DashboardOptions(bootstrap, port, zone, results, advisories, subscriptions, ticks, flushMs);
    }
}
