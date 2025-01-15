from mqtt_node_network.node import MQTTNode
from mqtt_node_network.metrics_node import MQTTMetricsNode


def test_config_file_init():

    node = MQTTNode.from_config_file(
        config_file="tests/config-test.toml", secrets_file="tests/test.env"
    )

    assert node.name == "test-node"
    assert node.node_id != "test-node"
    assert node.hostname == "localhost"
    assert node.node_type == "MQTTNode"
    assert node.port == 1883
    assert node._password == "super_secret_password"
    assert node._username == "test-user"

    metrics_node = MQTTMetricsNode.from_config_file(
        config_file="tests/config-test.toml", secrets_file="tests/test.env"
    )

    assert metrics_node.name == "test-node"
    assert metrics_node.node_id != "test-node"
    assert metrics_node.hostname == "localhost"
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
