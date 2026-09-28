package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Writes {@link EmaResult}s to {@code ema-results}: key = symbol, value = JSON (D-16). */
public class EmaResultSerializer implements KafkaRecordSerializationSchema<EmaResult> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public EmaResultSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            EmaResult result, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(
                topic,
                result.symbol.getBytes(StandardCharsets.UTF_8),
                toJson(result).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Field order follows docs/interfaces.md. {@link Double#toString} round-trips exactly, which the
     * 1e-9 oracle comparison relies on (D-18).
     */
    static String toJson(EmaResult r) {
        return new StringBuilder(192)
                .append("{\"symbol\":").append(quote(r.symbol))
                .append(",\"secType\":").append(quote(r.secType))
                .append(",\"windowEnd\":").append(r.windowEnd)
                .append(",\"close\":").append(r.close)
                .append(",\"ema38\":").append(r.ema38)
                .append(",\"ema100\":").append(r.ema100)
                .append(",\"maxIngestTs\":").append(r.maxIngestTs)
                .append(",\"emitTs\":").append(r.emitTs)
                .append('}')
                .toString();
    }

    static String quote(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\').append(c);
            } else if (c < 0x20) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.append('"').toString();
    }
}
