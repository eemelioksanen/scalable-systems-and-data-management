package fi.aalto.cse4780.trading;

import static fi.aalto.cse4780.trading.EmaCalculatorTest.millis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TickDeserializerTest {

    private final Map<String, Counter> counters = new HashMap<>();
    private final List<Tick> emitted = new ArrayList<>();
    private final TickDeserializer deserializer = new TickDeserializer(new TickParser());

    @BeforeEach
    void setUp() {
        MetricGroup metrics = new UnregisteredMetricsGroup() {
            @Override
            public Counter counter(String name) {
                return counter(name, new SimpleCounter());
            }

            @Override
            public <C extends Counter> C counter(String name, C counter) {
                counters.put(name, counter);
                return counter;
            }

            @Override
            public MetricGroup addGroup(String name) {
                return this;
            }
        };
        deserializer.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return metrics;
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
            }
        });
    }

    @Test
    void emitsPriceEventsWithOffsetAndIngestTime() {
        deserialize(42, 1_790_000_000_123L,
                "RDSA.NL,E,08-11-2021,09:00:01.234,19.280000,09:00:01.100,08-11-2021");

        assertEquals(1, emitted.size());
        Tick tick = emitted.get(0);
        assertEquals("RDSA.NL", tick.symbol);
        assertEquals(19.28, tick.price);
        assertEquals(millis("2021-11-08T09:00:01.100"), tick.eventTimeMillis);
        assertEquals(42, tick.sourceOrder);
        assertEquals(1_790_000_000_123L, tick.ingestTimeMillis);
        assertEquals(1, counters.get("priceEvents").getCount());
        assertEquals(0, counters.get("systemDateEvents").getCount());
    }

    @Test
    void countsPriceEventsDatedFromTheSystemDate() {
        deserialize(7, 0, "RDSA.NL,E,08-11-2021,09:00:01.234,19.280000,09:00:01.100,");

        assertEquals(1, emitted.size());
        assertEquals(millis("2021-11-08T09:00:01.100"), emitted.get(0).eventTimeMillis);
        assertEquals(1, counters.get("priceEvents").getCount());
        assertEquals(1, counters.get("systemDateEvents").getCount());
    }

    @Test
    void dropsAndCountsEveryOtherRecord() {
        deserialize(1, 0, "RDSA.NL,E,08-11-2021,09:00:01.456,,,");
        deserialize(2, 0, "RDSA.NL,E,08-11-2021,09:00:01.456,0.000000,09:00:01.100,");
        deserialize(3, 0, "RDSA.NL,E,08-11-2021,09:00:01,19.28,,");
        deserialize(4, 0, "RDSA.NL,E");
        deserialize(5, 0, null);

        assertEquals(0, emitted.size());
        assertEquals(0, counters.get("priceEvents").getCount());
        assertEquals(1, counters.get("nonPriceEvents").getCount());
        assertEquals(1, counters.get("nonPositivePriceEvents").getCount());
        assertEquals(1, counters.get("missingTimestampEvents").getCount());
        assertEquals(2, counters.get("malformedEvents").getCount());
    }

    @Test
    void retainsSourcePositionWithoutChangingPriceAdmission() {
        String value = "RDSA.NL,E,08-11-2021,09:00:01.234,19.28,09:00:01.100,";
        for (String source : List.of("2:17", "bad", "0:0", "-1:2", "0:999999999999999999999")) {
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "ticks", 0, 42, null, value.getBytes(StandardCharsets.UTF_8));
            record.headers().add("src", source.getBytes(StandardCharsets.US_ASCII));
            deserializer.deserialize(record, new ListCollector<>(emitted));
        }
        assertEquals(5, emitted.size());
        assertEquals(2, emitted.get(0).sourceFileIndex);
        assertEquals(17L, emitted.get(0).sourceLine);
        for (Tick tick : emitted.subList(1, emitted.size())) {
            assertNull(tick.sourceFileIndex);
            assertNull(tick.sourceLine);
        }
        assertEquals(5, counters.get("priceEvents").getCount());
    }

    private void deserialize(long offset, long timestamp, String value) {
        byte[] bytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        deserializer.deserialize(
                new ConsumerRecord<>("ticks", 0, offset, timestamp, TimestampType.CREATE_TIME,
                        7, bytes == null ? 0 : bytes.length,
                        "RDSA.NL".getBytes(StandardCharsets.UTF_8), bytes,
                        new RecordHeaders(), Optional.empty()),
                new ListCollector<>(emitted));
    }
}
