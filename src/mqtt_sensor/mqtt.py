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

import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe
from prometheus_client import Counter

logger = logging.getLogger(__name__)


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


class Node:
    _ids = itertools.count()

    def __init__(
        self,
        name=None,
        node_id=None,
        node_type=None,
    ):
        self.name = name
        self.node_id = node_id or self.get_id()
        self.node_type = node_type or self.__class__.__name__

    def get_id(self):
        # Return a unique id for each node
        return f"{self.node_type}_{next(self._ids)}"


class MQTTNode(Node):

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
        self, broker_config: MQTTBrokerConfig, name=None, node_id="", node_type=None
    ):
        super().__init__(name=name, node_id=node_id, node_type=node_type)
        self.client_id = node_id
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

    def print_msg(self, client, userdata, message):
        print("%s : %s" % (message.topic, message.payload))

    def subscribe(self, topic="#", qos=0):
        subscribe.callback(
            self.print_msg,
            topic,
            hostname=self.hostname,
            port=self.port,
            auth=self._auth,
            qos=qos,
            client_id=self.node_id,
        )

    def publish(self, topic, payload, qos=0, retain=False):
        if self.node_id:
            topic = f"{self.node_id}/{topic}"
        publish.single(
            payload=payload,
            topic=topic,
            hostname=self.hostname,
            port=self.port,
            auth=self._auth,
            qos=qos,
            retain=retain,
            client_id=self.node_id,
        )

    def on_message(self, client, userdata, message):
        self.node_messages_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        self.node_bytes_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc(len(message.payload))
        logger.info(f"Received message: {message.payload}")

    def on_publish(self, client, userdata, mid):
        self.node_messages_sent_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        logger.info(f"Published message: {mid}")

    def on_connect(self, client, userdata, flags, rc):
        logger.info(f"Connected with result code {rc}")
        client.subscribe("#")

    def on_disconnect(self, client, userdata, rc):
        logger.info(f"Disconnected with result code {rc}")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logger.info(f"Subscribed with mid {mid} and granted QoS {granted_qos}")

    def on_unsubscribe(self, client, userdata, mid):
        logger.info(f"Unsubscribed with mid {mid}")
