from __future__ import annotations
import time
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt import MQTTException
from paho.mqtt.subscribeoptions import SubscribeOptions


from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import (
    MQTTBrokerConfig,
    PacketTypes,
    MQTTPublishProperties,
    MQTTConnectProperties,
    SubscribeConfig,
)


base_config = MQTTBrokerConfig(
    username="rw",
    password="readwrite",
    keepalive=60,
    hostname="test.mosquitto.org",
    port=1884,
    timeout=1,
    reconnect_attempts=5,
    clean_session=0,
)

properties_default = {
    PacketTypes.CONNECT: MQTTConnectProperties(session_expiry_interval=0),
    PacketTypes.PUBLISH: MQTTPublishProperties(
        message_expiry_interval=0,
    ),
}

bool_topic = f"bool"
message_topic = f"messages"

subscribe_config = SubscribeConfig(
    topics=["+/" + t for t in [bool_topic, message_topic]],
    options=SubscribeOptions(
        qos=2,
        noLocal=False,
        retainAsPublished=True,
        retainHandling=1,
    ),
)


disconnect_reason = ReasonCode(packetType=PacketTypes.DISCONNECT, identifier=4)


def publish_message(node: MQTTNode, topic, payload, qos, retain=False):
    topic_full = f"{node.name}/{topic}"
    node.publish(topic=topic_full, payload=payload, qos=qos, retain=retain)
    logging.info(f"'{node.name}' published message on topic {topic}: {payload}")


class MessageCounter(MQTTNode):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.number_of_messages = 0
        self.client.on_connect = self.on_connect

    def on_message(self, client, userdata, message):
        self.number_of_messages += 1
        return super().on_message(client, userdata, message)


def create_node(name, subscribe_config, broker_config=None, properties=None):

    if broker_config is None:
        broker_config = base_config

    if properties is None:
        properties = properties_default

    node = MessageCounter(
        broker_config=broker_config,
        subscribe_config=subscribe_config,
        packet_properties=properties,
        name=name,
    )

    node.connect(ensure_connected=True)

    if not node.is_connected():
        raise MQTTException(f"Failed to connect {name} to broker.")

    return node


def disconnect_node(node: MQTTNode, reasoncode: ReasonCode):
    node.disconnect(ensure_disconnected=True, reasoncode=reasoncode)


def reconnect_node(node: MQTTNode):
    node.connect(ensure_connected=False)


def test_qos_level(qos=0):

    subscribe_config.options.QoS = qos

    subscriber = create_node(
        name="subscriber",
        subscribe_config=subscribe_config,
    )

    publisher = create_node(
        name="publisher",
        subscribe_config=None,
    )

    publish_message(
        publisher, topic=message_topic, payload="Message 1", qos=qos, retain=False
    )
    publish_message(
        publisher, topic=message_topic, payload="Message 2", qos=qos, retain=False
    )

    publish_message(publisher, topic=bool_topic, payload="True", qos=qos, retain=True)
    publish_message(publisher, topic=bool_topic, payload="False", qos=qos, retain=True)

    time.sleep(1)

    assert subscriber.number_of_messages == 4
    disconnect_node(subscriber, disconnect_reason)

    time.sleep(1)

    reconnect_node(subscriber)
    while not subscriber.is_connected():
        time.sleep(0.1)

    logging.info("Waiting 5s for any last minute messages to be received")

    time.sleep(5)

    logging.info("Test completed")


if __name__ == "__main__":
    test_qos_level(qos=0)
