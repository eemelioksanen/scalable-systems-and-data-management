package fi.aalto.cse4780.trading;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Turns {@code subscriptions} records into {@link SubscriptionUpdate}s (D-17, docs/interfaces.md).
 *
 * <p>The key is the trader ID. The value is {@code {"trader":"…","symbols":["…"]}}, where
 * {@code trader} is optional but must equal the key when present, or null for a tombstone.
 *
 * <p>Counters: {@code subscriptionUpdates}, {@code subscriptionTombstones} and
 * {@code malformedSubscriptions}. A malformed record is still emitted, with a null trader, because
 * {@link SubscriptionFilter} needs every position to tell when it has read the whole topic.
 */
public class SubscriptionDeserializer
        implements KafkaRecordDeserializationSchema<SubscriptionUpdate> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper json;
    private transient Counter updates;
    private transient Counter tombstones;
    private transient Counter malformed;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        json = new ObjectMapper();
        MetricGroup metrics = context.getMetricGroup().addGroup("subscriptions");
        updates = metrics.counter("subscriptionUpdates");
        tombstones = metrics.counter("subscriptionTombstones");
        malformed = metrics.counter("malformedSubscriptions");
    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<SubscriptionUpdate> out) {
        String trader = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        String[] symbols = null;
        if (trader == null || trader.isEmpty()) {
            trader = null;
            malformed.inc();
        } else if (record.value() == null) {
            tombstones.inc();
        } else {
            symbols = symbols(trader, record.value());
            if (symbols == null) {
                trader = null;
                malformed.inc();
            } else {
                updates.inc();
            }
        }
        out.collect(new SubscriptionUpdate(trader, symbols, record.partition(), record.offset()));
    }

    /** The symbol list in a record value, or null if the value is malformed. */
    private String[] symbols(String trader, byte[] value) {
        JsonNode root;
        try {
            root = json.readTree(value);
        } catch (IOException e) {
            return null;
        }
        if (root == null || !root.isObject()) {
            return null;
        }
        JsonNode named = root.get("trader");
        if (named != null && !(named.isTextual() && named.asText().equals(trader))) {
            return null;
        }
        JsonNode list = root.get("symbols");
        if (list == null || !list.isArray()) {
            return null;
        }
        String[] symbols = new String[list.size()];
        for (int i = 0; i < symbols.length; i++) {
            JsonNode symbol = list.get(i);
            if (!symbol.isTextual() || symbol.asText().isEmpty()) {
                return null;
            }
            symbols[i] = symbol.asText();
        }
        return symbols;
    }

    @Override
    public TypeInformation<SubscriptionUpdate> getProducedType() {
        return TypeInformation.of(SubscriptionUpdate.class);
    }
}
