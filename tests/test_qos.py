from __future__ import annotations
import socket
import time
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt import MQTTException
from paho.mqtt.subscribeoptions import SubscribeOptions
import pytest


from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import (
    MQTTBrokerConfig,
    PacketTypes,
    MQTTPublishProperties,
    MQTTConnectProperties,
    SubscribeConfig,
)


base_config = MQTTBrokerConfig(
    username="",
    password="",
    keepalive=60,
    hostname="localhost",
    port=1883,
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
        retainHandling=0,
    ),
)


disconnect_reason = ReasonCode(packetType=PacketTypes.DISCONNECT, identifier=128)


def publish_message(node: MQTTNode, topic, payload, qos, retain=False):
    topic_full = f"{node.name}/{topic}"
    node.publish(
        topic=topic_full, payload=payload, qos=qos, retain=retain, ensure_published=True
    )
    logging.info(f"'{node.name}' published message on topic {topic}: {payload}")


class MessageString:
    def __init__(self, message):
        self.message = message

    def __eq__(self, other):
        return self.message == other.message

    def __repr__(self):
        return f"{self.message.payload} / {self.message.qos} / {self.message.retain}"


class MessageCounter(MQTTNode):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.number_of_messages = 0
        # self.client.on_message = self.on_message
        # self.client.on_publish = self.on_publish
        self.messages_recvd = []
        self.messages_sent = []

    def on_message(self, client, userdata, message):
        self.number_of_messages += 1
        self.messages_recvd.append(MessageString(message))
        logging.info(f"Received message {message.payload} / {self.number_of_messages}")
        return super().on_message(client, userdata, message)

    def on_publish(self, client, userdata, mid, reason_code, properties):
        self.messages_sent.append(mid)
        logging.info(f"Published message {mid}")
        return super().on_publish(client, userdata, mid, reason_code, properties)


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


# def disconnect_node(node: MQTTNode, reasoncode: ReasonCode):
#     node.disconnect(reasoncode=reasoncode)
#     while node.is_connected():
#         time.sleep(0.1)


def disconnect_node_abruptly(client):
    """
    Simulate a non-graceful disconnect by forcefully closing the network socket.
    """
    # client.loop_stop()
    # client.disconnect()  # This sends a DISCONNECT packet; to truly simulate, skip this.
    # To simulate non-graceful, you might need to kill the thread or close the socket directly.
    # However, Paho doesn't provide a direct method to close the socket without sending DISCONNECT.
    # As a workaround, you can terminate the process or raise an exception.

    # For simulation purposes, we'll stop the loop without sending DISCONNECT
    # Note: In Paho, calling disconnect() sends a DISCONNECT packet. To avoid that:
    # You can directly close the socket (not recommended as it's not part of the public API)
    # Here's a workaround using a private attribute (use with caution):
    try:
        client._sock.shutdown(socket.SHUT_RDWR)
        client._sock.close()
        logging.info("Subscriber disconnected abruptly.")
    except Exception as e:
        logging.error(f"Error during abrupt disconnect: {e}")


def reconnect_node(node: MQTTNode):
    node.connect(ensure_connected=False)


# @pytest.mark.skip()
def test_qos_level(qos=1):

    subscribe_config.options.QoS = qos

    publisher = create_node(
        name="publisher",
        subscribe_config=None,
    )
    publish_message(publisher, topic=bool_topic, payload="False", qos=qos, retain=True)

    subscriber = create_node(
        name="subscriber",
        subscribe_config=subscribe_config,
    )

    publish_message(
        publisher, topic=message_topic, payload="Message 1", qos=qos, retain=False
    )
    publish_message(
        publisher, topic=message_topic, payload="Message 2", qos=qos, retain=False
    )

    publish_message(publisher, topic=bool_topic, payload="True1", qos=qos, retain=True)
    publish_message(publisher, topic=bool_topic, payload="False1", qos=qos, retain=True)

    logging.info("Waiting 5s for any last minute messages to be received")

    while subscriber.number_of_messages < 5:
        time.sleep(1)

    assert subscriber.number_of_messages == 5

    # logging.info("Test completed")

    disconnect_node_abruptly(subscriber.client)

    publish_message(
        publisher, topic=message_topic, payload="Message 3", qos=qos, retain=False
    )
    publish_message(
        publisher, topic=message_topic, payload="Message 4", qos=qos, retain=True
    )

    publish_message(publisher, topic=bool_topic, payload="True2", qos=qos, retain=True)
    publish_message(publisher, topic=bool_topic, payload="False2", qos=qos, retain=True)

    reconnect_node(subscriber)

    while not subscriber.is_connected():
        time.sleep(1)

    # publish_message(
    #     publisher, topic=message_topic, payload="Message 3", qos=qos, retain=False
    # )
    # publish_message(
    #     publisher, topic=message_topic, payload="Message 4", qos=qos, retain=False
    # )

    time.sleep(10)

    if qos == 0:
        assert subscriber.number_of_messages == 7
    elif qos == 1:
        assert subscriber.number_of_messages == 9
    elif qos == 2:
        assert subscriber.number_of_messages == 6


if __name__ == "__main__":
    test_qos_level(qos=1)
