package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Writes the crossover of an {@link EmaResult} to {@code advisories}: key = symbol, value = JSON
 * (D-16). Only results with a crossover reach this serializer.
 */
public class AdvisorySerializer implements KafkaRecordSerializationSchema<EmaResult> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public AdvisorySerializer(String topic) {
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

    /** Field order follows docs/interfaces.md, with the same number formatting as ema-results. */
    static String toJson(EmaResult r) {
        return new StringBuilder(192)
                .append("{\"symbol\":").append(EmaResultSerializer.quote(r.symbol))
                .append(",\"secType\":").append(EmaResultSerializer.quote(r.secType))
                .append(",\"windowEnd\":").append(r.windowEnd)
                .append(",\"type\":\"").append(r.crossover).append('"')
                .append(",\"ema38\":").append(r.ema38)
                .append(",\"ema100\":").append(r.ema100)
                .append(",\"maxIngestTs\":").append(r.maxIngestTs)
                .append(",\"emitTs\":").append(r.emitTs)
                .append('}')
                .toString();
    }
}
