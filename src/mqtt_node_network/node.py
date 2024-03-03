#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------
from __future__ import annotations
from dataclasses import dataclass
import logging
import itertools
import socket
from typing import Union

import paho.mqtt.client as mqtt
from mqtt import SubscribeOptions
from prometheus_client import Counter

logger = logging.getLogger(__name__)


def shorten_data(data: str, max_length: int = 75) -> str:
    """Shorten data to a maximum length."""
    if not isinstance(data, str):
        data = str(data)
    data = data.strip()
    return data[:max_length] + "..." if len(data) > max_length else data


def convert_bytes_to_human_readable(num: float) -> str:
    """Convert bytes to a human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024.0:
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} {unit}"


class NodeError(Exception):
    def __init__(self, message):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)


@dataclass
class MQTTBrokerConfig:
    username: str
    password: str
    keepalive: int
    hostname: str
    port: int


class MQTTNode:

    _ids = itertools.count()

    node_bytes_received_count = Counter(
        "node_bytes_received_total",
        "Total number of bytes received by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )
    node_bytes_sent_count = Counter(
        "node_bytes_sent_total",
        "Total number of bytes sent by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    node_messages_received_count = Counter(
        "node_messages_received_total",
        "Total number of messages received by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    node_messages_sent_count = Counter(
        "node_messages_sent_total",
        "Total number of messages sent by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    def __init__(
        self,
        broker_config: MQTTBrokerConfig,
        name=None,
        node_id=None,
        node_type=None,
        logger=None,
        client_id=None,
    ):
        self.name = name
        self.node_id = node_id or self._get_id()
        self.node_type = node_type or self.__class__.__name__
        self.client_id = client_id or self.node_id

        self.hostname: str = broker_config.hostname
        self.port: int = broker_config.port
        self.address = (broker_config.hostname, broker_config.port)
        self.keepalive: int = broker_config.keepalive

        self._username: str = broker_config.username
        self._password: str = broker_config.password
        self._auth: dict = {
            "username": broker_config.username,
            "password": broker_config.password,
        }

        # Initialize client
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt.MQTTv5,
        )
        self.client.username_pw_set(self._username, self._password)
        if logger:
            self.client.enable_logger(logger)

        # Set client callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_unsubscribe = self.on_unsubscribe
        # self.client.on_log = self.on_log

    def connect(self):
        self.client.connect(self.hostname, self.port, self.keepalive)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        return self

    def subscribe(self, topic: Union[str, tuple, list[tuple]] = "#", qos: int = 0):
        """
        Subscribe to a topic
        :topic: Can be a string, a tuple, or a list of tuple of format (topic, qos). Both topic and qos must
               be present in all of the tuples.
        :qos, options and properties: Not used.

        e.g. subscribe("my/topic", 2)
        subscribe("my/topic", options=SubscribeOptions(qos=2))
        subscribe(("my/topic", 1))
        subscribe([("my/topic", 0), ("another/topic", 2)])
        """

        if isinstance(topic, str):
            topic = (topic, SubscribeOptions(qos))
        elif isinstance(topic, tuple):
            topic = tuple((topic_, SubscribeOptions(qos)) for topic_ in topic)
        result = self.client.subscribe(topic)
        if result[0] == 4:
            logger.error(
                f"Failed to subscribe to topic(s): {[topic_ for topic_ in topic]}",
                extra={"reason_code": mqtt.error_string(result[0])},
            )
        else:
            logger.info(f"Subscribed to topic(s): {[topic_ for topic_ in topic]}")

    def unsubscribe(self, topic: Union[str, list[str]], properties=None):
        """
        :param topic: A single string, or list of strings that are the subscription
            topics to unsubscribe from.
        :param properties: (MQTT v5.0 only) a Properties instance setting the MQTT v5.0 properties
            to be included. Optional - if not set, no properties are sent.
        """
        return self.client.unsubscribe(topic)

    def publish(self, topic, payload, qos=0, retain=False):
        return self.client.publish(topic, payload, qos, retain)

    def loop_forever(self):
        self.client.loop_forever()

    # Callbacks
    # ***************************************************************************

    def on_pre_connect(self, client, userdata):
        logger.info(f"Connecting to broker at {client.host}:{client.port}")

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"Connected to broker at {client.host}:{client.port}")
        # client.subscribe(topic)

    def on_connect_fail(self, client, userdata):
        logger.error(f"Failed to connect to broker at {client.host}:{client.port}")

    def on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        logger.info(f"Disconnected with result code {reason_code}")

    def on_message(self, client, userdata, message):
        self.node_messages_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        self.node_bytes_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc(len(message.payload))
        logger.info(f"Received message: {shorten_data(message.payload.decode())}")

    def on_publish(self, client, userdata, mid, reason_code, properties):
        self.node_messages_sent_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        logger.debug("Published message: {}".format(mid))

    def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        logger.info("Subscribed to topic")

    def on_unsubscribe(self, client, userdata, mid, properties, reason_codes):
        logger.info("Unsubscribed from topic")

    def on_log(self, client, userdata, level, buf):
        logger.debug("Log: {}".format(buf))

    def _get_id(self):
        # Return a unique id for each node
        return f"{self.node_type}_{next(self._ids)}"

    def __del__(self):
        self.client.disconnect()
        logger.info(f"Disconnected from broker at {self.hostname}:{self.port}")
