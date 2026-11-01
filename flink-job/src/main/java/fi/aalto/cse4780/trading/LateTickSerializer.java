package fi.aalto.cse4780.trading;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LateTickSerializer implements KafkaRecordSerializationSchema<Tick> {

    private static final long serialVersionUID = 1L;
    private final String topic;

    public LateTickSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Tick tick, KafkaSinkContext context, Long timestamp) {
        String json = "{\"sourceFileIndex\":" + tick.sourceFileIndex
                + ",\"sourceLine\":" + tick.sourceLine
                + ",\"symbol\":" + EmaResultSerializer.quote(tick.symbol)
                + ",\"windowEnd\":" + EmaCalculator.windowEnd(tick.eventTimeMillis) + '}';
        return new ProducerRecord<>(topic, tick.symbol.getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8));
    }
}
