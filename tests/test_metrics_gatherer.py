from dataclasses import asdict
import pytest
from mqtt_node_network.metrics_node import parse_topic


def test_topic_parser():
    # Example usage
    topic = "pzero/sensorbox_lower/temperature/sensorA/0"
    structure = "machine/module/measurement/field*"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature",
        "field": "sensorA-0",
    }

    structure = "machine/module/measurement/field"

    with pytest.raises(ValueError):
        parsed_dict = parse_topic(topic, structure)

    structure = "machine/module/field*"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "field": "temperature-sensorA-0",
    }

    structure = "machine/module/measurement*"
    assert not (
        parse_topic(topic, structure)
        == {
            "machine": "pzero",
            "module": "sensorbox_lower",
            "measurement": "temperature",
            "field": "sensorA-0",
        }
    )
    assert parse_topic(topic, structure) == {
        "machine": "pzero",
        "module": "sensorbox_lower",
        "measurement": "temperature-sensorA-0",
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
    from mqtt_node_network.metrics_node import parse_payload_to_metric

    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/measurement/field*"
    value = 25.5

    metric = parse_payload_to_metric(value, topic, structure)
    assert metric == {
        "measurement": "temperature",
        "fields": {"sensorA-0": 25.5},
        "time": metric["time"],
        "tags": {"machine": "bedroom", "module": "bedside_table"},
    }


def test_extended_topic_structure():

    topic = "pzero/normal/sensor/sensorbox_lower/temperature/sensorA/0"
    structure = "machine/level/datatype/module/measurement/field*"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "pzero",
        "level": "normal",
        "datatype": "sensor",
        "module": "sensorbox_lower",
        "measurement": "temperature",
        "field": "sensorA-0",
    }

    from mqtt_node_network.metrics_node import parse_payload_to_metric

    metric = parse_payload_to_metric(25.5, topic, structure)
    assert metric == {
        "measurement": "temperature",
        "fields": {"sensorA-0": 25.5},
        "time": metric["time"],
        "tags": {
            "machine": "pzero",
            "level": "normal",
            "datatype": "sensor",
            "module": "sensorbox_lower",
        },
    }


def test_incorrect_topic_length():
    topic = "pzero/normal/sensor/test_sensor/sensorA"
    structure = "machine/level/datatype/module/measurement/field*"

    with pytest.raises(ValueError):
        parse_topic(topic, structure)
