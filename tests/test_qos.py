from __future__ import annotations
import time
import pytest
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

from mqtt_node_network.configuration import initialize_logging

logger = initialize_logging("./config/logging.yaml")

LOG_FILEPATH_TEST = "test_qos.log"
LOG_FILEPATH_LIB = "mqtt_node_network.log"

base_config = MQTTBrokerConfig(
    username="elevated",
    password="dhbi723b23ivbg7gbifdhubw",
    keepalive=60,
    hostname="lm26consys1.gf.local",
    port=1883,
    timeout=0.2,
    reconnect_attempts=10,
    clean_session=1,
)
# base_config = MQTTBrokerConfig(
#     username="rw",
#     password="readwrite",
#     keepalive=60,
#     hostname="test.mosquitto.org",
#     port=1883,
#     timeout=5,
#     reconnect_attempts=10,
#     clean_session=3,
# )

properties_default = {
    PacketTypes.CONNECT: MQTTConnectProperties(session_expiry_interval=0),
    PacketTypes.PUBLISH: MQTTPublishProperties(
        message_expiry_interval=0,
    ),
}

logger = logging.getLogger("mqtt_qos_test")
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler(LOG_FILEPATH_TEST)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

handler.setFormatter(formatter)
logger.addHandler(handler)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

logger_lib = logging.getLogger("mqtt_node_network.node")
handler = logging.FileHandler(LOG_FILEPATH_LIB)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger_lib.addHandler(handler)

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
    logger.info(f"'{node.name}' published message on topic {topic}: {payload}")


def message_callback(client, userdata, message):
    logger.info(
        f"'{client._client_id.decode()}' received message on topic '{message.topic}': {message.payload.decode()} | DUP: {message.dup} | QoS: {message.qos} | RETAIN: {message.retain}"
    )


def on_connect(client, userdata, flags, reason_code, properties):
    logger.info(
        f"'{client._client_id.decode()}' connected to broker with reason code '{reason_code}'"
    )


def on_subscribe(client, userdata, mid, granted_qos, properties):
    logger.info(
        f"'{client._client_id.decode()}' subscribed to topic with QoS '{granted_qos}'"
    )


def on_disconnect(client, userdata, flags, reason_code, properties):
    logger.info(
        f"'{client._client_id.decode()}' disconnected from broker with reason code '{reason_code}'"
    )


def create_node(name, subscribe_config, broker_config=None, properties=None):

    if broker_config is None:
        broker_config = base_config

    if properties is None:
        properties = properties_default

    node = MQTTNode(
        broker_config=broker_config,
        subscribe_config=subscribe_config,
        properties=properties,
        name=name,
        node_id="",
    )
    # node.client.on_connect = on_connect
    # node.client.on_disconnect = on_disconnect
    # node.client.on_subscribe = on_subscribe

    node.connect(ensure_connected=True)

    node.restore_subscriptions()
    # if not node.is_connected():
    #     raise MQTTException(f"Failed to connect {name} to broker.")

    # if subscribe_config is not None:
    #     for topic in subscribe_config.topics:
    #         node.message_callback_add(topic, message_callback)

    return node


def disconnect_node(node: MQTTNode, reasoncode: ReasonCode):
    node.disconnect(ensure_disconnected=True, reasoncode=reasoncode)


def reconnect_node(node: MQTTNode):
    node.connect(ensure_connected=False)


def test_qos_level(qos=0):

    # FLush log file
    with open(LOG_FILEPATH_TEST, "w") as f:
        f.write("")

    subscribe_config.options.QoS = qos

    subscriber = create_node(
        name="subscriber",
        subscribe_config=subscribe_config,
    )

    publisher = create_node(
        name="publisher",
        subscribe_config=None,
    )

    while not subscriber.is_connected():
        time.sleep(0.1)

    disconnect_node(subscriber, disconnect_reason)

    time.sleep(0.1)

    publish_message(
        publisher, topic=message_topic, payload="Message 1", qos=qos, retain=False
    )
    publish_message(
        publisher, topic=message_topic, payload="Message 2", qos=qos, retain=False
    )

    publish_message(publisher, topic=bool_topic, payload="True", qos=qos, retain=True)
    publish_message(publisher, topic=bool_topic, payload="False", qos=qos, retain=True)

    time.sleep(0.1)
    reconnect_node(subscriber)

    while not subscriber.is_connected():
        time.sleep(0.1)

    time.sleep(0.1)

    disconnect_node(subscriber, disconnect_reason)

    time.sleep(1)

    reconnect_node(subscriber)
    while not subscriber.is_connected():
        time.sleep(0.1)

    logger.info("Waiting 5s for any last minute messages to be received")

    time.sleep(5)

    logger.info("Test completed")


if __name__ == "__main__":
    test_qos_level(qos=0)
