from mqtt_node_network.metrics_gatherer import parse_topic


def test_topic_parser():
    # Example usage
    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/measurement/field"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "bedroom",
        "module": "bedside_table",
        "measurement": "temperature",
        "field": "sensorA_0",
    }

    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/field"

    parsed_dict = parse_topic(topic, structure)
    assert parsed_dict == {
        "machine": "bedroom",
        "module": "bedside_table",
        "field": "temperature_sensorA_0",
    }

    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/measurement"
    assert not (
        parse_topic(topic, structure)
        == {
            "machine": "bedroom",
            "module": "bedside_table",
            "measurement": "temperature",
            "field": "sensorA_0",
        }
    )


def test_parse_data_to_metric():
    # Example usage
    from mqtt_node_network.metrics_gatherer import parse_payload_to_metric

    topic = "bedroom/bedside_table/temperature/sensorA/0"
    structure = "machine/module/measurement/field"
    value = 25.5

    metric = parse_payload_to_metric(value, topic, structure)
    assert metric == {
        "measurement": "temperature",
        "fields": {"sensorA_0": 25.5},
        "time": metric["time"],
        "tags": {"machine": "bedroom", "module": "bedside_table"},
    }
