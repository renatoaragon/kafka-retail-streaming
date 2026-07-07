from retail_stream.events import (
    CATEGORIES,
    SALE,
    STOCK,
    GeneratorConfig,
    event_key,
    generate_events,
)


def _all(count, **kw):
    return list(generate_events(count, GeneratorConfig(**kw)))


def test_generator_is_deterministic_per_seed():
    assert _all(50, seed=7) == _all(50, seed=7)


def test_different_seeds_diverge():
    assert _all(50, seed=1) != _all(50, seed=2)


def test_count_is_respected():
    assert len(_all(0)) == 0
    assert len(_all(123)) == 123


def test_events_have_expected_shape():
    events = _all(200, seed=3)
    for e in events:
        assert e["event_type"] in (SALE, STOCK)
        assert e["category"] in CATEGORIES
        assert e["ts"].endswith("Z")
        if e["event_type"] == SALE:
            assert 1 <= e["quantity"] <= 5
            assert e["unit_price"] > 0
            assert e["order_id"].startswith("O")
        else:
            assert e["quantity_delta"] != 0
            assert e["sku"].startswith("SKU")


def test_timestamps_are_monotonic():
    ts = [e["ts"] for e in _all(100, seed=5, step_seconds=5)]
    assert ts == sorted(ts)


def test_both_event_types_are_produced():
    types = {e["event_type"] for e in _all(300, seed=9)}
    assert types == {SALE, STOCK}


def test_event_key_prefers_business_id():
    assert event_key({"order_id": "O1", "event_id": "sale-0"}) == "O1"
    assert event_key({"sku": "SKU9", "event_id": "stock-0"}) == "SKU9"
    assert event_key({"event_id": "x"}) == "x"  # fallback
