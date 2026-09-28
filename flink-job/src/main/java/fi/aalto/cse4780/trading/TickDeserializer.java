package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Turns {@code ticks} records into {@link Tick}s. Only price events are emitted; every other record
 * is counted per {@link TickParser.Outcome} and dropped here, before the keyed calculation.
 *
 * <p>The record offset becomes {@link Tick#sourceOrder} and the record timestamp becomes
 * {@link Tick#ingestTimeMillis}.
 */
public class TickDeserializer implements KafkaRecordDeserializationSchema<Tick> {

    private static final long serialVersionUID = 1L;

    private final TickParser parser;

    private transient Counter priceEvents;
    private transient Counter nonPriceEvents;
    private transient Counter missingTimestampEvents;
    private transient Counter malformedEvents;

    public TickDeserializer(TickParser parser) {
        this.parser = parser;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        MetricGroup ticks = context.getMetricGroup().addGroup("ticks");
        priceEvents = ticks.counter("priceEvents");
        nonPriceEvents = ticks.counter("nonPriceEvents");
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
        switch (parser.parse(new String(record.value(), StandardCharsets.UTF_8), tick)) {
            case PRICE -> {
                tick.sourceOrder = record.offset();
                tick.ingestTimeMillis = record.timestamp();
                priceEvents.inc();
                out.collect(tick);
            }
            case NON_PRICE -> nonPriceEvents.inc();
            case MISSING_TIMESTAMP -> missingTimestampEvents.inc();
            case MALFORMED -> malformedEvents.inc();
        }
    }

    @Override
    public TypeInformation<Tick> getProducedType() {
        return TypeInformation.of(Tick.class);
    }
}
