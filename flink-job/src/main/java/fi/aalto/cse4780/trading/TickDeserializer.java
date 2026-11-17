package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Turns {@code ticks} records into {@link Tick}s. Only price events are emitted. Every other
 * record is counted and dropped here, before the keyed calculation.
 *
 * <p>These counters partition all records: {@code priceEvents}, {@code nonPriceEvents},
 * {@code nonPositivePriceEvents}, {@code missingTimestampEvents} and {@code malformedEvents}.
 * {@code systemDateEvents} counts the price events dated from {@code Date} (D-25), a subset of
 * {@code priceEvents}.
 *
 * <p>The record offset becomes {@link Tick#sourceOrder} and the record timestamp becomes
 * {@link Tick#ingestTimeMillis}. Values are CSV or binary, as the replayer wrote them (D-42);
 * both are classified by the same rules.
 */
public class TickDeserializer implements KafkaRecordDeserializationSchema<Tick> {

    private static final long serialVersionUID = 1L;
    private static final Pattern SOURCE_POSITION = Pattern.compile("[0-9]+:[0-9]+");

    private final TickParser parser;
    private final Variant.TicksEncoding encoding;

    private transient Counter priceEvents;
    private transient Counter systemDateEvents;
    private transient Counter nonPriceEvents;
    private transient Counter nonPositivePriceEvents;
    private transient Counter missingTimestampEvents;
    private transient Counter malformedEvents;

    public TickDeserializer(TickParser parser, Variant.TicksEncoding encoding) {
        this.parser = parser;
        this.encoding = encoding;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        MetricGroup ticks = context.getMetricGroup().addGroup("ticks");
        priceEvents = ticks.counter("priceEvents");
        systemDateEvents = ticks.counter("systemDateEvents");
        nonPriceEvents = ticks.counter("nonPriceEvents");
        nonPositivePriceEvents = ticks.counter("nonPositivePriceEvents");
        missingTimestampEvents = ticks.counter("missingTimestampEvents");
        malformedEvents = ticks.counter("malformedEvents");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tick> out) {
        if (record.value() == null) {
            malformedEvents.inc();
            return;
        }
        Tick tick = new Tick();
        TickParser.Outcome outcome = encoding == Variant.TicksEncoding.CSV
                ? parser.parse(new String(record.value(), StandardCharsets.UTF_8), tick)
                : parser.parseBinary(record.value(), tick);
        switch (outcome) {
            case PRICE, PRICE_SYSTEM_DATE -> {
                if (outcome == TickParser.Outcome.PRICE_SYSTEM_DATE) {
                    systemDateEvents.inc();
                }
                tick.sourceOrder = record.offset();
                tick.ingestTimeMillis = record.timestamp();
                readSource(record, tick);
                priceEvents.inc();
                out.collect(tick);
            }
            case NON_PRICE -> nonPriceEvents.inc();
            case NON_POSITIVE_PRICE -> nonPositivePriceEvents.inc();
            case MISSING_TIMESTAMP -> missingTimestampEvents.inc();
            case MALFORMED -> malformedEvents.inc();
        }
    }

    private static void readSource(ConsumerRecord<byte[], byte[]> record, Tick tick) {
        var header = record.headers().lastHeader("src");
        if (header == null || header.value() == null) {
            return;
        }
        String source = new String(header.value(), StandardCharsets.US_ASCII);
        if (!SOURCE_POSITION.matcher(source).matches()) {
            return;
        }
        int separator = source.indexOf(':');
        try {
            int file = Integer.parseInt(source.substring(0, separator));
            long line = Long.parseLong(source.substring(separator + 1));
            if (line > 0) {
                tick.sourceFileIndex = file;
                tick.sourceLine = line;
            }
        } catch (NumberFormatException ignored) {
            return;
        }
    }

    @Override
    public TypeInformation<Tick> getProducedType() {
        return TypeInformation.of(Tick.class);
    }
}
