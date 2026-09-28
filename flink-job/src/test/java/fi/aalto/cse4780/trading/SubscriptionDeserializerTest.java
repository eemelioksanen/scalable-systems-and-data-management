package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

class SubscriptionDeserializerTest {

    private final Map<String, Counter> counters = new HashMap<>();
    private final List<SubscriptionUpdate> emitted = new ArrayList<>();
    private final SubscriptionDeserializer deserializer = new SubscriptionDeserializer();

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
    void readsTheSymbolListWithItsPosition() {
        deserialize(3, 17, "trader-0042", "{\"trader\":\"trader-0042\",\"symbols\":[\"RDSA.NL\",\"*\"]}");

        SubscriptionUpdate update = single();
        assertEquals("trader-0042", update.trader);
        assertArrayEquals(new String[] {"RDSA.NL", "*"}, update.symbols);
        assertEquals(3, update.partition);
        assertEquals(17, update.offset);
        assertEquals(1, counters.get("subscriptionUpdates").getCount());
    }

    @Test
    void takesTheTraderFromTheKeyWhenTheValueOmitsIt() {
        deserialize(0, 0, "trader-1", "{\"symbols\":[]}");

        SubscriptionUpdate update = single();
        assertEquals("trader-1", update.trader);
        assertArrayEquals(new String[0], update.symbols);
    }

    @Test
    void readsATombstoneAsARemoval() {
        deserialize(0, 5, "trader-1", null);

        SubscriptionUpdate update = single();
        assertEquals("trader-1", update.trader);
        assertNull(update.symbols);
        assertEquals(1, counters.get("subscriptionTombstones").getCount());
    }

    @Test
    void keepsThePositionOfMalformedRecordsButNoChange() {
        String[][] malformed = {
            {null, "{\"symbols\":[\"A.FR\"]}"},
            {"", "{\"symbols\":[\"A.FR\"]}"},
            {"trader-1", "{\"trader\":\"trader-2\",\"symbols\":[\"A.FR\"]}"},
            {"trader-1", "not json"},
            {"trader-1", ""},
            {"trader-1", "[\"A.FR\"]"},
            {"trader-1", "{\"trader\":\"trader-1\"}"},
            {"trader-1", "{\"symbols\":\"A.FR\"}"},
            {"trader-1", "{\"symbols\":[\"A.FR\",1]}"},
            {"trader-1", "{\"symbols\":[\"\"]}"},
        };
        for (int i = 0; i < malformed.length; i++) {
            deserialize(0, i, malformed[i][0], malformed[i][1]);
        }

        assertEquals(malformed.length, emitted.size());
        for (int i = 0; i < malformed.length; i++) {
            SubscriptionUpdate update = emitted.get(i);
            assertNull(update.trader, () -> "accepted " + update);
            assertEquals(i, update.offset);
        }
        assertEquals(malformed.length, counters.get("malformedSubscriptions").getCount());
        assertEquals(0, counters.get("subscriptionUpdates").getCount());
    }

    private SubscriptionUpdate single() {
        assertEquals(1, emitted.size(), () -> "emitted: " + emitted);
        return emitted.get(0);
    }

    private void deserialize(int partition, long offset, String key, String value) {
        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        deserializer.deserialize(
                new ConsumerRecord<>("subscriptions", partition, offset, 0L,
                        TimestampType.CREATE_TIME, keyBytes == null ? 0 : keyBytes.length,
                        valueBytes == null ? 0 : valueBytes.length, keyBytes, valueBytes,
                        new RecordHeaders(), Optional.empty()),
                new ListCollector<>(emitted));
    }
}
