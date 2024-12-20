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
