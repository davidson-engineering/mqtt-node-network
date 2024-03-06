from dataclasses import asdict
import pytest
from mqtt_node_network.metrics_gatherer import parse_topic


def test_topic_parser():
    # Example usage
    topic = "pzero/sensorbox_lower/temperature/sensorA/0"
    structure = "machine/module/measurement/field*"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature",
        "field": "sensorA_0",
    }

    structure = "machine/module/measurement/field"

    with pytest.raises(ValueError):
        parsed_dict = parse_topic(topic, structure)

    structure = "machine/module/field*"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "field": "temperature_sensorA_0",
    }

    structure = "machine/module/measurement*"
    assert not (
        parse_topic(topic, structure)
        == {
            "machine": "pzero",
            "module": "sensorbox_lower",
            "measurement": "temperature",
            "field": "sensorA_0",
        }
    )
    assert parse_topic(topic, structure) == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature_sensorA_0",
    }

    topic = "pzero/sensorbox_lower/temperature/sensorA"
    structure = "machine/module/measurement/field"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature",
        "field": "sensorA",
    }
    topic = "pzero/sensorbox_lower/temperature"
    structure = "machine/module/measurement/field"

    with pytest.raises(ValueError):
        parsed_dict = parse_topic(topic, structure)

    topic = "pzero/sensorbox_lower/temperature/"
    structure = "machine/module/measurement/field"
    with pytest.raises(ValueError):
        parsed_dict = parse_topic(topic, structure)

    topic = "pzero/sensorbox_lower/temperature/IR_sensorA/"
    structure = "machine/module/measurement/field/"
    assert parse_topic(topic, structure) == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature",
        "field": "IR_sensorA",
    }


def test_parse_data_to_metric():
    # Example usage
    from mqtt_node_network.metrics_gatherer import parse_payload_to_metric

    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/measurement/field*"
    value = 25.5

    metric = parse_payload_to_metric(value, topic, structure)
    assert asdict(metric) == {
        "measurement": "temperature",
        "fields": {"sensorA_0": 25.5},
        "time": metric["time"],
        "tags": {"machine": "bedroom", "module": "bedside_table"},
    }
