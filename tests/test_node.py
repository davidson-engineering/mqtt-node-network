from collections import deque
import time
import typing

import pytest
from mqtt_node_network.node import MQTTNode, MQTTBrokerConfig
from mqtt_node_network.metrics_node import MQTTMetricsNode

from conftest import TEST_NODE_NAME


def test_config_file_init_node(broker_config):

    node = MQTTNode.from_config_file(
        config_file="tests/config-test.toml",
        # secrets_file="tests/test.env",
        broker_config=broker_config,
        node_id="",  # Blank node_id
    )

    assert node.name == TEST_NODE_NAME
    # Check that a new node_id is generated, as a blank node_id is passed
    assert node.node_id != TEST_NODE_NAME
    assert node.hostname == broker_config.hostname
    assert node.node_type == "MQTTNode"
    assert node.port == broker_config.port
    assert node._password == broker_config.password
    assert node._username == broker_config.username


def test_config_file_init_metrics_node(broker_config):
    metrics_node = MQTTMetricsNode.from_config_file(
        config_file="tests/config-test.toml",
        # secrets_file="tests/test.env",
        broker_config=broker_config,
        node_id="my_metrics_node_7",
    )

    assert metrics_node.name == TEST_NODE_NAME
    assert metrics_node.node_id != TEST_NODE_NAME
    assert metrics_node.node_id == "my_metrics_node_7"
    assert metrics_node.hostname == broker_config.hostname
    assert metrics_node.node_type == "MQTTMetricsNode"
    assert metrics_node._username == broker_config.username
    assert metrics_node._password == broker_config.password

    assert metrics_node.topic_structure == "module/measurement/field*"
    assert metrics_node.datatype == typing.Dict
    assert isinstance(metrics_node.buffer, deque)


def test_subscribe_to_topic(mqtt_test_client):

    mqtt_test_client.unsubscribe_all()

    assert len(mqtt_test_client.subscriptions) == 0

    DEVICE_ID = mqtt_test_client.node_id
    SUBSCRIBE_TOPICS = [f"{DEVICE_ID}/test/topic", f"{DEVICE_ID}/test/topic2"]
    QOS = 0
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    mqtt_test_client.ensure_connection()
    assert mqtt_test_client.is_connected()
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    mqtt_test_client.restore_subscriptions()
    assert len(mqtt_test_client.subscriptions) == 2

    # Resubscribe to one of the same topics
    SUBSCRIBE_STRING = SUBSCRIBE_TOPICS[0]
    mqtt_test_client.subscribe(topic=SUBSCRIBE_STRING, qos=QOS)
    # Ensure that the subscription is not duplicated
    assert len(mqtt_test_client.subscriptions) == 2

    # Resubscribe to the same topic with a tuple
    SUBSCRIBE_TUPLE = (SUBSCRIBE_STRING, QOS)
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TUPLE)
    # Ensure that the subscription is not duplicated
    assert len(mqtt_test_client.subscriptions) == 2

    # Resubscribe to the same topic with a list
    SUBSCRIBE_TOPICS = [SUBSCRIBE_TOPICS[0], SUBSCRIBE_TOPICS[0]]
    mqtt_test_client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    # Ensure that the subscription is not duplicated
    assert len(mqtt_test_client.subscriptions) == 2

    # Subscribe to a new topic
    NEW_TOPIC = f"{DEVICE_ID}/test/topic3"
    mqtt_test_client.subscribe(topic=NEW_TOPIC, qos=QOS)
    assert len(mqtt_test_client.subscriptions) == 3

    mqtt_test_client.unsubscribe_all()
    assert len(mqtt_test_client.subscriptions) == 0

    mqtt_test_client.client.disconnect()

    assert not mqtt_test_client.is_connected()


def test_publish_message(mqtt_test_client):

    DEVICE_ID = mqtt_test_client.node_id
    PUBLISH_TOPIC = f"{DEVICE_ID}/test/topic"
    QOS = 0

    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)
    mqtt_test_client.publish(topic=PUBLISH_TOPIC, payload="Hello, World!", qos=QOS)

    mqtt_test_client.client.disconnect()
    assert not mqtt_test_client.is_connected()


# @pytest.mark.skip()
def test_message_callback_add(mqtt_test_client: MQTTNode):

    message_received = False

    def test_callback(client, userdata, message):
        global message_received
        message_received = True

    topic = "123456789/topic"

    mqtt_test_client.ensure_connection()

    # Add the callback, and assert that it was added to the client
    mqtt_test_client.message_callback_add(topic, test_callback, qos=0)
    assert (
        mqtt_test_client.client._on_message_filtered[topic].__name__
        == test_callback.__name__
    )
    # Ensure that the callback is called when a message is received
    mqtt_test_client.publish(
        topic=topic, payload="Hello, World!", qos=0, ensure_published=True
    )
    time.sleep(5)
    assert message_received

    # Remove the callback
    mqtt_test_client.message_callback_remove(topic)
    # Ensure that the callback is removed
    with pytest.raises(KeyError):
        mqtt_test_client.client._on_message_filtered[topic]


@pytest.mark.skip()
def test_ensure_published(mqtt_test_client: MQTTNode):

    topic = "123456789/topic"
    payload = "Hello, World!"
    qos = 2

    message_received = False

    def test_callback(client, userdata, message):
        nonlocal message_received
        message_received = True

    mqtt_test_client.message_callback_add(topic, test_callback)
    mqtt_test_client.ensure_connection()
    mqtt_test_client.publish(
        topic=topic, payload=payload, qos=qos, ensure_published=True
    )
    assert message_received

    mqtt_test_client.client.disconnect()
    assert not mqtt_test_client.is_connected()
