import pytest
import logging

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import MQTTBrokerConfig

base_config = MQTTBrokerConfig(
    username="",
    password="",
    hostname="test.moquitto.org",
    port=1883,
)

logger_publisher = logging.getLogger("publisher")
logger_publisher.setLevel(logging.DEBUG)
logger_subscriber = logging.getLogger("subscriber")
logger_subscriber.setLevel(logging.DEBUG)

publisher_topic = f"publisher"


def publish_callback(client, userdata, message):
    logger_publisher.debug(f"Received message on topic {message.topic}: {message.payload}")

def subscribe_callback(client, userdata, message):
    logger_subscriber.debug(f"Published message on topic {message.topic}: {message.payload}")


def create_node(name, callback, subscribe_config, broker_config=base_config):

    node = MQTTNode(
        broker_config=broker_config, subscribe_config=subscribe_config, name=name
    )
    node.connect(
    for topic in subscribe_config.topics:
        node.message_callback_add(topic, callback)


def test_qos_level0():

    subscribe_config = {
        "topics": [f"{publisher_topic}/data"],
        "qos": 0,
    }

    create_node("subscriber", subscribe_callback, subscribe_config)

    publisher = MQTTNode(
        broker_config=base_config, name="publisher"
    )

    publisher.connect()
    publisher.publish(topic=f"{publisher_topic}/qos0", payload="QoS 0", qos=0)

    publisher.loop_forever(timeout=1)
    publisher.client.disconnect()