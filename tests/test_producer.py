import json

from retail_stream.events import GeneratorConfig, generate_events
from retail_stream.producer import publish, serialize


class FakeProducer:
    """Captures produced records instead of talking to a broker."""

    def __init__(self):
        self.records = []
        self.flushed = False

    def produce(self, topic, key, value):
        self.records.append((topic, key, value))

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def test_serialize_roundtrips_and_is_stable():
    event = {"b": 2, "a": 1, "event_type": "sale"}
    payload = serialize(event)
    assert json.loads(payload) == event
    # sort_keys makes the encoding deterministic regardless of dict order.
    assert serialize({"a": 1, "b": 2, "event_type": "sale"}) == payload


def test_publish_sends_all_events_and_flushes():
    events = list(generate_events(25, GeneratorConfig(seed=11)))
    fake = FakeProducer()

    sent = publish(fake, "retail.events", events)

    assert sent == 25
    assert len(fake.records) == 25
    assert fake.flushed is True
    assert all(topic == "retail.events" for topic, _, _ in fake.records)


def test_publish_keys_and_payloads_match_events():
    events = list(generate_events(10, GeneratorConfig(seed=1)))
    fake = FakeProducer()

    publish(fake, "t", events)

    for event, (_, key, value) in zip(events, fake.records):
        assert json.loads(value) == event
        expected_key = (event.get("order_id") or event.get("sku")).encode("utf-8")
        assert key == expected_key
