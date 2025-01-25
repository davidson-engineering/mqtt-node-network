from mqtt_node_network.node import MQTTNode, MQTTBrokerConfig
from mqtt_node_network.metrics_node import MQTTMetricsNode


def test_config_file_init():

    broker_config = MQTTBrokerConfig(
        username="rw",
        password="readwrite",
        keepalive=60,
        hostname="test.mosquitto.org",
        port=1884,
        timeout=1,
        reconnect_attempts=5,
        clean_session=1,
    )

    node = MQTTNode.from_config_file(
        config_file="tests/config-test.toml",
        secrets_file="tests/test.env",
        broker_config=broker_config,
    )

    assert node.name == "test_node_987123"
    assert node.node_id != "test_node_987123"
    assert node.hostname == broker_config.hostname
    assert node.node_type == "MQTTNode"
    assert node.port == broker_config.port
    assert node._password == broker_config.password
    assert node._username == broker_config.username

    metrics_node = MQTTMetricsNode.from_config_file(
        config_file="tests/config-test.toml",
        secrets_file="tests/test.env",
        broker_config=broker_config,
    )

    assert metrics_node.name == "test_node_987123"
    assert metrics_node.node_id != "test_node_987123"
    assert metrics_node.hostname == broker_config.hostname
    assert metrics_node.node_type == "MQTTMetricsNode"


def test_subscribe_to_topic(mqtt_test_client):

    DEVICE_ID = mqtt_test_client.node_id
    SUBSCRIBE_TOPICS = [f"{DEVICE_ID}/test/topic", f"{DEVICE_ID}/test/topic2"]
    QOS = 0

    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert mqtt_test_client.is_connected()
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    mqtt_test_client.restore_subscriptions()
    assert len(mqtt_test_client.subscriptions) == 2

    SUBSCRIBE_STRING = SUBSCRIBE_TOPICS[0]
    mqtt_test_client.subscribe(topic=SUBSCRIBE_STRING, qos=QOS)
    assert len(mqtt_test_client.subscriptions) == 2

    SUBSCRIBE_TUPLE = (SUBSCRIBE_STRING, QOS)
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TUPLE)
    assert len(mqtt_test_client.subscriptions) == 2

    SUBSCRIBE_TOPICS = [SUBSCRIBE_TOPICS[0], SUBSCRIBE_TOPICS[0]]
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    assert len(mqtt_test_client.subscriptions) == 2

    mqtt_test_client.client.disconnect()
    assert not mqtt_test_client.is_connected()
