"""Time topic rereads with the exporter's decoder, without sorting or writing Parquet."""
import json
import time

from kafka import KafkaConsumer
from ema_check.export import _read_topic, EMA_SCHEMA, ADVISORY_SCHEMA, LATE_SCHEMA, PRICE_EVENT_SCHEMA
from ema_check.latency import price_event, zone_offset_ms

consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None, enable_auto_commit=False,
                         auto_offset_reset='earliest', max_partition_fetch_bytes=16 << 20)
report = []
offset_ms = zone_offset_ms('+02:00')
try:
    for topic, schema in [('ema-results', EMA_SCHEMA), ('advisories', ADVISORY_SCHEMA),
                          ('late-ticks', LATE_SCHEMA), ('ticks', PRICE_EVENT_SCHEMA)]:
        start = time.monotonic()
        kwargs = {'decode': lambda rec: price_event(rec.value, offset_ms)} if topic == 'ticks' else {}
        frame, info = _read_topic(consumer, topic, schema, 30, **kwargs)
        report.append({'topic': topic, 'elapsedSeconds': time.monotonic() - start, **info})
finally:
    consumer.close()
print(json.dumps(report, indent=2))
