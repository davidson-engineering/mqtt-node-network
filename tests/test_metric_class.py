import pytest
from mqtt_node_network.metrics_node import Metric


def test_metric():
    # test item assignment
    metric = Metric(
        measurement="temperature",
        fields={"value": 25.0},
        time=1629000000,
        tags={"location": "bedroom"},
    )
    assert metric["measurement"] == "temperature"
    assert metric["fields"] == {"value": 25.0}
    assert metric["time"] == 1629000000
    assert metric["tags"] == {"location": "bedroom"}
    metric["measurement"] = "humidity"
    assert metric["measurement"] == "humidity"
    metric["fields"] = {"value": 50.0}
    assert metric["fields"] == {"value": 50.0}
    metric["time"] = 1629000001
    assert metric["time"] == 1629000001
    metric["tags"] = {"location": "kitchen"}
    assert metric["tags"] == {"location": "kitchen"}


def test_metric_initialization():
    # test initialization with different values
    metric = Metric(
        measurement="pressure",
        fields={"value": 101.3},
        time=1629000002,
        tags={"location": "office"},
    )
    assert metric["measurement"] == "pressure"
    assert metric["fields"] == {"value": 101.3}
    assert metric["time"] == 1629000002
    assert metric["tags"] == {"location": "office"}


def test_metric_update():
    # test updating fields and tags
    metric = Metric(
        measurement="temperature",
        fields={"value": 25.0},
        time=1629000000,
        tags={"location": "bedroom"},
    )
    metric.fields["value"] = 26.0
    assert metric.fields["value"] == 26.0
    metric.tags["location"] = "living room"
    assert metric.tags["location"] == "living room"


def test_metric_invalid_key():
    # test accessing invalid key
    metric = Metric(
        measurement="temperature",
        fields={"value": 25.0},
        time=1629000000,
        tags={"location": "bedroom"},
    )
    try:
        metric["invalid_key"]
    except KeyError:
        assert True
    else:
        assert False


def test_metric_deletion():
    # test deleting fields and tags
    metric = Metric(
        measurement="temperature",
        fields={"value": 25.0},
        time=1629000000,
        tags={"location": "bedroom"},
    )
    del metric.fields
    with pytest.raises(AttributeError):
        "fields" not in metric
    del metric.tags
    with pytest.raises(AttributeError):
        "tags" not in metric
