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
import socket
import threading
from typing import Dict, List, Tuple, Union
import time
import asyncio
import copy

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from prometheus_client import Counter, Gauge
from mqtt_node_network.configure import MQTTBrokerConfig, LatencyMonitoringConfig

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


def extend_or_append(list_topics, topic):
    for item in topic:
        if isinstance(item, tuple):
            extend_or_append(list_topics, item)
        else:
            list_topics.append(item)


def parse_properties_dict(properties: dict) -> Properties:

    publish_properties = Properties(PacketTypes.PUBLISH)

    if isinstance(properties, dict):
        for key, value in properties.items():
            if not isinstance(value, str):
                value = str(value)
            publish_properties.UserProperty = (key, value)
    elif isinstance(properties, Properties):
        publish_properties = properties
    else:
        raise ValueError(
            "User property must be a dictionary or a paho.mqtt.properties.Properties instance"
        )
    return publish_properties


def parse_topic(topic: Union[str, list, tuple], qos: int = 0) -> Union[list, tuple]:
    if isinstance(topic, str):
        return (topic, mqtt.SubscribeOptions(qos))
    elif isinstance(topic, tuple):
        return (topic[0], mqtt.SubscribeOptions(qos))
    elif isinstance(topic, list):
        return [(topic_, mqtt.SubscribeOptions(qos)) for topic_ in topic]
    else:
        raise ValueError("Topic must be a string, tuple or list")


def dict_to_user_properties(properties_dict: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Convert a dictionary to a list of tuples for user properties.

    :param properties_dict: Dictionary containing user properties
    :return: List of tuples where each tuple is (key, value)
    """
    return [(key, value) for key, value in properties_dict.items()]


def user_properties_to_dict(user_properties: List[Tuple[str, str]]) -> Dict[str, str]:
    """
    Convert a list of tuples (user properties) to a dictionary.

    :param user_properties: List of tuples where each tuple is (key, value)
    :return: Dictionary containing user properties
    """
    return dict(user_properties)


class NodeError(Exception):
    def __init__(self, message):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)


class MQTTNode:

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

    node_client_to_client_latency = Gauge(
        "node_client_to_client_latency",
        "Estimated latency between two clients connected through the MQTT broker",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    def __init__(
        self,
        broker_config: MQTTBrokerConfig,
        name=None,
        node_id="",
        node_type=None,
        logger=None,
        subscriptions: list = None,
        latency_config: LatencyMonitoringConfig = None,
    ):
        self.name = name
        self.node_type = node_type or self.__class__.__name__
        self.node_id = node_id or self._get_id()
        self.client_id = node_id
        self.subscriptions = subscriptions or []

        self.hostname: str = broker_config.hostname
        self.port: int = broker_config.port
        self.address = (broker_config.hostname, broker_config.port)
        self.keepalive: int = broker_config.keepalive
        self.timeout: int = broker_config.timeout
        self.reconnect_attempts: int = broker_config.reconnect_attempts

        self._username: str = broker_config.username
        self._password: str = broker_config.password
        self._auth: dict = {
            "username": broker_config.username,
            "password": broker_config.password,
        }

        # Initialize client
        client_id = self.name or self.node_id
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        self.client.username_pw_set(self._username, self._password)
        if logger:
            self.client.enable_logger(logger)

        # Set latency metrics
        self.latency_config = (
            copy.deepcopy(latency_config)
            if latency_config
            else LatencyMonitoringConfig()
        )
        self.latency_config.response_topic = (
            f"{client_id}/{self.latency_config.response_topic}"
        )
        self.latency_config.request_topic = (
            f"{client_id}/{self.latency_config.request_topic}"
        )
        if self.latency_config.enabled:
            # Subscribe to latency monitoring topics
            self.subscriptions.append(self.latency_config.response_topic)
            self.subscriptions.append(self.latency_config.request_topic)

            # Add callbacks for latency monitoring to take action when these messages are received
            self.client.message_callback_add(
                self.latency_config.response_topic, self._update_latency_metric
            )
            self.client.message_callback_add(
                self.latency_config.request_topic, self._send_response
            )

        # Set client callbacks
        # self.client.on_pre_connect = self.on_pre_connect
        self.client.on_connect = self.on_connect
        self.client.on_connect_fail = self.on_connect_fail
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

        # self.client.on_subscribe = self.on_subscribe
        # self.client.on_unsubscribe = self.on_unsubscribe
        # self.client.on_log = self.on_log

        self.is_connected = self.client.is_connected
        self.loop_forever = self.client.loop_forever

    def connect(self):
        self.loop_start()
        if self.is_connected() is False:
            if self.client.connect(self.hostname, self.port, self.keepalive) != 0:
                logger.warning(
                    f"Connection attempt to {self.hostname}:{self.port} failed",
                    extra={
                        "node_id": self.node_id,
                        "node_name": self.name,
                        "node_type": self.node_type,
                        "host": self.hostname,
                    },
                )
                return self
            self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        # self.ensure_connection()

        return self

    def subscribe(self, topic: Union[str, tuple, list], qos: int = 0):
        """
        Subscribe to a topic
        :topic: str | tuple | list
        :qos: quality of service, 0 | 1 | 2

        """

        topic = parse_topic(topic, qos)

        result = self.client.subscribe(topic)

        if result[0] != 0:
            error_string = mqtt.error_string(result[0])
            logger.error(
                f"{error_string}, failed to subscribe to topic: {topic}",
                extra={
                    "error_code": error_string,
                    "node_id": self.node_id,
                    "node_name": self.name,
                    "node_type": self.node_type,
                    "host": self.hostname,
                },
            )
        else:
            logger.info(
                f"Subscribed to topic: {topic}",
                extra={
                    "topic": topic,
                    "node_id": self.node_id,
                    "node_name": self.name,
                    "node_type": self.node_type,
                    "host": self.hostname,
                },
            )

        # Add the topic to the list of subscriptions
        self.add_subscription_topic(topic)

    def unsubscribe(self, topic: Union[str, list[str]], properties=None):
        """
        :param topic: A single string, or list of strings that are the subscription
            topics to unsubscribe from.
        :param properties: (MQTT v5.0 only) a Properties instance setting the MQTT v5.0 properties
            to be included. Optional - if not set, no properties are sent.
        """
        # remove from self.subscriptions
        if isinstance(topic, list):
            for t in topic:
                self.subscriptions.remove(t)
        elif isinstance(topic, str):
            self.subscriptions.remove(topic)
        return self.client.unsubscribe(topic)

    def add_subscription_topic(self, topic: Union[str, list, tuple]):

        def append_topic(topic):
            assert isinstance(topic, str)
            if topic not in self.subscriptions:
                self.subscriptions.append(topic)

        if isinstance(topic, list):
            for t in topic:
                if isinstance(t, tuple):
                    append_topic(t[0])
                else:
                    append_topic(t)

        elif isinstance(topic, tuple):
            append_topic(topic[0])
        elif isinstance(topic, str):
            append_topic(topic)

    def restore_subscriptions(self, qos: int = 1):
        for topic in self.subscriptions:
            self.subscribe(topic, qos=qos)

    def ensure_connection(self):
        if self.is_connected() is True:
            return
        reconnects = 1
        while self.is_connected() is False:
            try:
                self.client.reconnect()
            except (ConnectionRefusedError, ValueError):
                logger.error(
                    f"Failed to reconnect to broker at {self.hostname}:{self.port}",
                    extra={
                        "node_id": self.node_id,
                        "node_name": self.name,
                        "node_type": self.node_type,
                        "host": self.hostname,
                    },
                )
            reconnects += 1
            logger.info(
                f"Retry attempt #{reconnects} in {self.timeout}s",
                extra={
                    "node_id": self.node_id,
                    "node_name": self.name,
                    "node_type": self.node_type,
                    "host": self.hostname,
                },
            )
            time.sleep(self.timeout)

    def publish(self, topic, payload, qos=0, retain=False, properties=None):
        self.ensure_connection()
        if properties:
            properties = parse_properties_dict(properties)
        return self.client.publish(topic, payload, qos, retain, properties=properties)

    def publish_every(
        self, topic, payload_func, qos=0, retain=False, properties=None, interval=1
    ):
        while True:
            payload = payload_func()
            self.publish(topic, payload, qos, retain, properties)
            time.sleep(interval)

    async def publish_every_async(
        self, topic, payload_func, qos=0, retain=False, properties=None, interval=1
    ):
        while True:
            payload = payload_func()
            self.publish(topic, payload, qos, retain, properties)
            await asyncio.sleep(interval)

    def loop_forever(self):
        self.client.loop_forever()
        if self.latency_config.enabled:
            self.start_periodic_latency_check()
        return self

    def loop_start(self):
        self.client.loop_start()
        if self.latency_config.enabled:
            self.start_periodic_latency_check()
        return self

    def start_periodic_latency_check(self):
        # Periodically send ping and publish latency
        def periodic_request():
            while True:
                self._send_request()
                time.sleep(self.latency_config.interval)

        latency_thread = threading.Thread(target=periodic_request)
        latency_thread.start()

    # Callbacks
    # ***************************************************************************

    # def on_pre_connect(self, client, userdata):
    # logger.info(
    #     f"Connecting to broker at {client.host}:{client.port}",
    #     extra={
    #         "node_id": self.node_id,
    #         "node_name": self.name,
    #         "node_type": self.node_type,
    #         "host": self.hostname,
    #     },
    # )

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(
            f"Connected to broker at {client.host}:{client.port}",
            extra={
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )
        self.restore_subscriptions()

    def on_connect_fail(self, client, userdata):
        logger.error(
            f"Failed to connect to broker at {client.host}:{client.port}",
            extra={
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )

    def on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        logger.info(f"Disconnected with result code: {reason_code}")

    def on_message(self, client, userdata, message):

        self.node_messages_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()

        self.node_bytes_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc(len(message.payload))

        logger.info(
            f"Received message on topic '{message.topic}': {shorten_data(message.payload.decode())}",
            extra={
                "topic": message.topic,
                "qos": message.qos,
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )

    def on_publish(self, client, userdata, mid, reason_code, properties):
        self.node_messages_sent_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        logger.debug(
            f"Published message #{mid}",
            extra={
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )

    def _update_latency_metric(self, client, userdata, message):
        # Calculate the latency between clients
        time_sent = float(
            user_properties_to_dict(message.properties.UserProperty).get("time_sent")
        )
        time_received = float(
            user_properties_to_dict(message.properties.UserProperty).get(
                "time_received"
            )
        )
        latency = (time_received - time_sent) * 1000  # latency in ms

        self.node_client_to_client_latency.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).set(latency)

        if self.latency_config.log_enabled:
            # Log the latency
            logger.info(
                f"Client to client latency {latency:.4f} ms",
                extra={
                    "node_id": self.node_id,
                    "node_name": self.name,
                    "node_type": self.node_type,
                    "host": self.hostname,
                },
            )

    # def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
    #  self.logger.info("Subscribed to topic")

    # def on_unsubscribe(self, client, userdata, mid, properties, reason_codes):
    #  self.logger.info("Unsubscribed from topic")

    def on_log(self, client, userdata, level, buf):
        logger.debug(
            "Log: {}".format(buf),
            extra={
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )

    def _get_id(self):
        # Return a unique id for each node
        return f"{self.node_type}_{time.time_ns()}"

    def _send_request(self):
        # Send a ping message and attach the time sent as a user property
        properties = Properties(PacketTypes.PUBLISH)
        properties.ResponseTopic = self.latency_config.response_topic
        properties.UserProperty = dict_to_user_properties(
            {"node_id": self.node_id, "time_sent": str(time.time())}
        )

        self.publish(
            self.latency_config.request_topic,
            payload="ping",
            qos=self.latency_config.qos,
            properties=properties,
        )

    def _send_response(self, client, userdata, message):
        # Send a response message with the same properties as the request
        properties = Properties(PacketTypes.PUBLISH)
        properties.UserProperty = message.properties.UserProperty
        # Add the time the message was received
        properties.UserProperty.append(("time_received", str(time.time())))

        self.publish(
            message.properties.ResponseTopic,
            payload="pong",
            qos=self.latency_config.qos,
            properties=properties,
        )

    def __del__(self):
        self.client.disconnect()
        logger.info(f"Disconnected from broker at {self.hostname}:{self.port}")
