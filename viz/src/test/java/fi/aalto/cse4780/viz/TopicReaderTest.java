package fi.aalto.cse4780.viz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.kafka.clients.consumer.MockConsumer;
import static fi.aalto.cse4780.viz.KafkaFixtures.supply;

import org.junit.jupiter.api.Test;

class TopicReaderTest {

    static String json(String symbol, long windowEnd, double ema38, double ema100) {
        return "{\"symbol\":\"" + symbol + "\",\"secType\":\"E\",\"windowEnd\":" + windowEnd
                + ",\"close\":10.0,\"ema38\":" + ema38 + ",\"ema100\":" + ema100
                + ",\"maxIngestTs\":1,\"emitTs\":2}";
    }

    @Test
    void readsEveryPartitionIntoTheStoreAndCountsMalformedRecords() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("ema-results", 2);
        Store store = new Store();
        TopicReader results = TopicReader.results(supply(consumer), "ema-results", store);

        assertTrue(results.pollOnce(Duration.ZERO));
        consumer.addRecord(KafkaFixtures.record("ema-results", 0, 0, 0, json("A.FR", StoreTest.T0, 2, 1)));
        consumer.addRecord(KafkaFixtures.record("ema-results", 1, 0, 0, json("B.NL", StoreTest.T0, 1, 2)));
        consumer.addRecord(KafkaFixtures.record("ema-results", 1, 1, 0, "garbage"));
        results.pollOnce(Duration.ZERO);

        assertEquals(2, results.read());
        assertEquals(1, results.malformed());
        assertEquals(2, store.symbolCount());
    }

    @Test
    void waitsWhileTheTopicDoesNotExist() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("other", 1);
        TopicReader results = TopicReader.results(supply(consumer), "ema-results", new Store());

        assertFalse(results.pollOnce(Duration.ZERO));
    }

    @Test
    void aBrokerThatCannotBeReachedYetIsRetriedNotFatal() {
        int[] attempts = {0};
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("ema-results", 1);
        TopicReader results = TopicReader.results(() -> {
            if (attempts[0]++ == 0) {
                throw new org.apache.kafka.common.KafkaException("Failed to construct kafka consumer");
            }
            return consumer;
        }, "ema-results", new Store());

        org.junit.jupiter.api.Assertions.assertThrows(org.apache.kafka.common.KafkaException.class,
                () -> results.pollOnce(Duration.ZERO));
        assertTrue(results.pollOnce(Duration.ZERO));
    }

    static String advisory(String symbol, long windowEnd, String type) {
        return "{\"symbol\":\"" + symbol + "\",\"secType\":\"E\",\"windowEnd\":" + windowEnd
                + ",\"type\":\"" + type + "\",\"ema38\":2.0,\"ema100\":1.0,\"maxIngestTs\":1,\"emitTs\":2}";
    }

    @Test
    void readsAdvisoriesIntoTheStore() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("advisories", 1);
        Store store = new Store();
        TopicReader reader = TopicReader.advisories(supply(consumer), "advisories", store);

        assertTrue(reader.pollOnce(Duration.ZERO));
        consumer.addRecord(KafkaFixtures.record("advisories", 0, 0, 0, advisory("A.FR", StoreTest.T0, "BUY")));
        consumer.addRecord(KafkaFixtures.record("advisories", 0, 1, 0, advisory("A.FR", StoreTest.T0, "HOLD")));
        reader.pollOnce(Duration.ZERO);

        assertEquals(1, reader.read());
        assertEquals(1, reader.malformed());
        assertEquals(1, store.advisoryCount());
    }

    @Test
    void readsSubscriptionsByTraderIncludingTombstones() {
        MockConsumer<String, String> consumer = KafkaFixtures.consumerWithTopic("subscriptions", 1);
        Subscriptions subs = new Subscriptions();
        TopicReader reader = TopicReader.subscriptions(supply(consumer), "subscriptions", subs);

        assertTrue(reader.pollOnce(Duration.ZERO));
        consumer.addRecord(KafkaFixtures.record("subscriptions", 0, 0, 0, "t1", "{\"symbols\":[\"A.FR\"]}"));
        consumer.addRecord(KafkaFixtures.record("subscriptions", 0, 1, 0, "t2", "{\"symbols\":[\"*\"]}"));
        consumer.addRecord(KafkaFixtures.record("subscriptions", 0, 2, 0, "t2", null));
        consumer.addRecord(KafkaFixtures.record("subscriptions", 0, 3, 0, null, "{\"symbols\":[]}"));
        reader.pollOnce(Duration.ZERO);

        assertEquals(3, reader.read());
        assertEquals(1, reader.malformed());
        assertEquals(java.util.Map.of("t1", java.util.List.of("A.FR")), subs.view().traders());
    }
}
