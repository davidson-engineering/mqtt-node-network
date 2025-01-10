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
import logging
from pathlib import Path
import socket
import threading
from typing import Dict, List, NoReturn, Optional, Tuple, Union
import time
import asyncio
import copy

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.enums import MQTTErrorCode
from prometheus_client import Counter, Gauge

from mqtt_node_network.configuration import (
    LatencyMonitoringConfig,
    initialize_config,
)


class NodeLoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = self.extra.copy()
        if "extra" in kwargs:
            extra.update(kwargs["extra"])
        kwargs["extra"] = extra
        return msg, kwargs


# Initialize your logger and adapter
logger = logging.getLogger(__name__)
node_logger = NodeLoggingAdapter(
    logger,
    {
        "node_id": None,
        "node_name": None,
        "node_type": None,
        "host": None,
    },
)


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


def extend_or_append(list_topics: List[str], topic: Union[str, Tuple]) -> None:
    """
    Recursively extend or append topics to a list.

    :param list_topics: A list of topic strings.
    :param topic: A topic to be added, which can be a string or tuple.
    """
    for item in topic:
        if isinstance(item, tuple):
            extend_or_append(list_topics, item)
        else:
            list_topics.append(item)


def parse_properties_dict(properties: Dict[str, Union[str, int]]) -> Properties:
    """
    Convert a dictionary into MQTT Properties.

    :param properties: Dictionary containing properties.
    :return: MQTT Properties object.
    """
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


def parse_topic(
    topic: Union[str, List[str], Tuple[str, ...]],
    qos: Optional[int] = None,
    options: mqtt.SubscribeOptions = None,
) -> Union[List[Tuple[str, mqtt.SubscribeOptions]], Tuple[str, mqtt.SubscribeOptions]]:
    """
    Parse a topic string, list, or tuple and apply MQTT subscription options.

    :param topic: A single topic or list/tuple of topics.
    :param qos: Quality of Service level (optional).
    :return: Parsed topic(s) with subscription options applied.
    """
    qos = qos or 0
    options = options or mqtt.SubscribeOptions(qos)
    if options.QoS != qos:
        options.QoS = qos
        logger.warning(
            f"Overriding QoS value in options with value {qos}",
        )
    if isinstance(topic, str):
        return (topic, options)
    elif isinstance(topic, tuple):
        return (topic[0], options)
    elif isinstance(topic, list):
        return [(topic_, options) for topic_ in topic]
    else:
        raise ValueError("Topic must be a string, tuple or list")


def dict_to_user_properties(properties_dict: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Convert a dictionary to a list of tuples for user properties.

    :param properties_dict: Dictionary containing user properties.
    :return: List of tuples where each tuple is (key, value).
    """
    return [(key, value) for key, value in properties_dict.items()]


def user_properties_to_dict(user_properties: List[Tuple[str, str]]) -> Dict[str, str]:
    """
    Convert a list of tuples (user properties) to a dictionary.

    :param user_properties: List of tuples where each tuple is (key, value).
    :return: Dictionary containing user properties.
    """
    return dict(user_properties)


class NodeError(Exception):
    """
    Exception raised for errors in the MQTTNode.
    """

    def __init__(self, message: str):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)


class MQTTNode:
    """
    A base class representing an MQTT Node, with integrated Prometheus metrics.
    """

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

    @classmethod
    def from_config_file(
        cls,
        config_file: Union[str, Path],
        secrets_file: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> MQTTNode:
        """
        Instantiate an MQTTNode from a configuration file.

        :param config_file: Path to the configuration file.
        :param secrets_file: Path to the secrets file (optional).
        :param kwargs: Additional keyword arguments. See MQTTNode.__init__ for details. These will override the config file.
        :return: An initialized MQTTNode instance.
        """
        config = initialize_config(config=config_file, secrets=secrets_file)[
            cls.__name__
        ]
        # Combine the configuration from the file with any additional keyword arguments
        # The keyword arguments will override the configuration file
        combined_args = {**config, **kwargs}
        return cls(**combined_args)

    def __init__(
        self,
        broker_config: MQTTBrokerConfig,  # type: ignore
        name: str,
        node_id: Optional[str] = None,
        subscribe_config: SubscribeConfig = None,  # type: ignore
        latency_config: LatencyMonitoringConfig = None,  # type: ignore
    ):
        """
        Initialize an MQTTNode instance.

        :param broker_config: The configuration for the MQTT broker.
        :param name: The name of the node.
        :param node_id: A unique identifier for the node (optional).
        :param subscribe_config: Configuration for subscribed topics.
        :param latency_config: Configuration for latency monitoring.
        """
        self.name = name
        self.node_type = self.__class__.__name__
        self.node_id = node_id if node_id else self._get_id()
        self.subscriptions = subscribe_config.topics if subscribe_config else []
        self.subscribe_qos = subscribe_config.qos if subscribe_config else 0

        self.hostname: str = broker_config.hostname
        self.port: int = broker_config.port
        self.address = (broker_config.hostname, broker_config.port)
        self.keepalive: int = broker_config.keepalive
        self.timeout: int = broker_config.timeout
        self.reconnect_attempts: int = broker_config.reconnect_attempts

        self._username: str = broker_config.username
        self._password: str = broker_config.password
        self._auth: Dict[str, str] = {
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

        # self.client.enable_logger(logger)

        # Set latency metrics
        self._latency_thread = None
        self._stop_event = threading.Event()
        self.latency_config = copy.deepcopy(latency_config) or LatencyMonitoringConfig()
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
                self.latency_config.request_topic, self._send_latency_response
            )

        # Set client callbacks
        self.client.on_pre_connect = self.on_pre_connect
        self.client.on_connect = self.on_connect
        self.client.on_connect_fail = self.on_connect_fail
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

        # self.client.on_subscribe = self.on_subscribe
        # self.client.on_unsubscribe = self.on_unsubscribe
        # self.client.on_log = self.on_log

        self.is_connected = self.client.is_connected
        # self.loop_forever = self.client.loop_forever

        self.logger = NodeLoggingAdapter(
            logger,
            {
                "node_id": self.node_id,
                "node_name": self.name,
                "node_type": self.node_type,
                "host": self.hostname,
            },
        )

        self.restore_subscriptions()

    def connect(self):
        if self.is_connected() is False:
            error_code = self.client.connect(self.hostname, self.port, self.keepalive)
            if error_code != 0:
                self.logger.warning(
                    f"Connection attempt to {self.hostname}:{self.port} failed"
                )
                return self
            self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        # self.ensure_connection()
        return self

    def subscribe(
        self,
        topic: Union[str, tuple, list],
        qos: Optional[int] = None,
        options: mqtt.SubscribeOptions = None,
    ):
        """
        Subscribe to a topic
        :topic: str | tuple | list
        :qos: quality of service, 0 | 1 | 2

        """

        topic = parse_topic(topic, qos, options)

        result = self.client.subscribe(topic)

        if result[0] != 0:
            error_string = mqtt.error_string(result[0])
            self.logger.error(
                f"{error_string}, failed to subscribe to topic: {topic}",
                extra={
                    "error_code": error_string,
                    "qos": qos,
                },
            )
        else:
            self.logger.info(
                f"Subscribed to topic: {topic}",
                extra={
                    "topic": topic,
                    "qos": qos,
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
        """
        Ensure that the client is connected to the broker.
        Blocking function that will attempt to reconnect if the client is not connected.
        """
        if self.is_connected() is True:
            return
        reconnects = 1
        while self.is_connected() is False:
            try:
                self.client.reconnect()
            except (ConnectionRefusedError, ValueError):
                self.logger.error(
                    f"Failed to reconnect to broker at {self.hostname}:{self.port}",
                )
            reconnects += 1
            self.logger.info(
                f"Retry attempt #{reconnects} in {self.timeout}s",
            )
            time.sleep(self.timeout)

    def publish(self, topic, payload, qos=0, retain=False, properties=None):
        self.ensure_connection()
        if properties:
            properties = parse_properties_dict(properties)
        return self.client.publish(topic, payload, qos, retain, properties=properties)

    def publish_every(
        self, topic, payload_func, qos=0, retain=False, properties=None, interval=1
    ) -> NoReturn:
        """
        Publish a message every interval seconds.

        This is a blocking function, and will run indefinitely.
        :topic: str - The topic to publish to
        :payload_func: function - A function that returns the payload to publish
        :qos: int - The Quality of Service level
        :retain: bool - Whether to retain the message
        :properties: dict - MQTT properties
        :interval: int - The interval in seconds
        """

        while True:
            payload = payload_func()
            self.publish(topic, payload, qos, retain, properties)
            time.sleep(interval)

    async def publish_every_async(
        self, topic, payload_func, qos=0, retain=False, properties=None, interval=1
    ) -> NoReturn:
        while True:
            payload = payload_func()
            self.publish(topic, payload, qos, retain, properties)
            await asyncio.sleep(interval)

    def check_loop_running(self):
        if self.client._thread is not None and self.client._thread.is_alive():
            return True
        return False

    def loop_forever(self, timeout: int = 1, reconnect_delay: int = 5) -> NoReturn:
        """
        Continuously checks connection status, keeps the client alive, and handles reconnections.
        If latency monitoring is enabled, starts periodic latency checks.

        Args:
            timeout (int): Time (in seconds) to wait between connection checks.
            reconnect_delay (int): Time (in seconds) to wait before attempting a reconnection.
        """
        if self.latency_config.enabled:
            self.start_periodic_latency_check()

        self.logger.info("Entering main loop with reconnection handling.")
        self.ensure_connection()
        try:
            while True:
                if self.is_connected():
                    self.logger.debug("Connection active. Maintaining connection.")
                    time.sleep(timeout)
                else:
                    self.logger.warning("Connection lost. Attempting to reconnect...")
                    try:
                        self.connect()
                        self.ensure_connection()
                        self.logger.info("Reconnected successfully.")
                    except Exception as e:
                        self.logger.error(f"Reconnection failed: {e}")
                        self.logger.info(f"Retrying in {reconnect_delay} seconds...")
                        time.sleep(reconnect_delay)
        except KeyboardInterrupt:
            self.logger.info("Loop interrupted by user. Stopping...")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in the main loop: {e}")
        finally:
            self.__del__()

    def loop_start(self) -> MQTTNode:

        error_code = self.client.loop_start()

        if error_code != 0:
            self.logger.error(
                f"Failed to start loop: {mqtt.error_string(error_code)}",
            )
        if self.latency_config.enabled:
            self.start_periodic_latency_check()

        return self

    def loop_stop(self) -> MQTTNode:
        error_code = self.client.loop_stop()
        if error_code != 0:
            self.logger.error(
                f"Failed to stop loop: {mqtt.error_string(error_code)}",
            )
        return self

    def start_periodic_latency_check(self):
        # Periodically send ping and publish latency
        if self._latency_thread and self._latency_thread.is_alive():
            self.logger.warning(
                f"Thread '{self.node_id}-latency_thread' already exists"
            )
            return

        def periodic_request() -> NoReturn:
            while not self._stop_event.is_set():
                try:
                    self._send_latency_request()
                except Exception as e:
                    self.logger.error(f"Error during latency request: {e}")
                time.sleep(self.latency_config.interval)

        # Start a new thread
        self.logger.info(
            "Starting latency monitoring thread",
            extra={
                "thread_name": f"{self.node_id}-latency_thread",
            },
        )
        self._stop_event.clear()
        self._latency_thread = threading.Thread(
            target=periodic_request,
            name=f"{self.node_id}-latency_thread",
            daemon=True,  # Optional: Make thread a daemon so it stops with the main program
        )
        self._latency_thread.start()

    def stop_periodic_latency_check(self):
        # Stop the periodic latency check thread
        if self._latency_thread and self._latency_thread.is_alive():
            self.logger.info(
                "Stopping latency monitoring thread",
                extra={
                    "thread_name": f"{self.node_id}-latency_thread",
                },
            )
            self._stop_event.set()
            self._latency_thread.join()

    # Callbacks
    # ***************************************************************************

    def on_pre_connect(self, client, userdata):
        if not self.check_loop_running():
            self.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties):

        self.logger.info(
            f"Connected to broker at {client.host}:{client.port}",
        )
        if not flags.session_present:
            logger.debug(
                "No session present. Restoring subscriptions ...",
            )
            self.restore_subscriptions()

    def on_connect_fail(self, client, userdata):
        self.logger.error(
            f"Failed to connect to broker at {client.host}:{client.port}",
        )

    def on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        self.logger.info(
            f"Disconnected with result code: {reason_code}",
        )

    def on_message(self, client, userdata, message):

        self.node_messages_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()

        self.node_bytes_received_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc(len(message.payload))

        self.logger.info(
            f"Received message on topic '{message.topic}': {shorten_data(message.payload.decode())}",
            extra={
                "topic": message.topic,
                "qos": message.qos,
            },
        )

    def on_publish(self, client, userdata, mid, reason_code, properties):
        self.node_messages_sent_count.labels(
            self.node_id, self.name, self.node_type, self.hostname
        ).inc()
        self.logger.debug(f"Published message #{mid}")

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
            self.logger.info(f"Client to client latency {latency:.4f} ms")

    # def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
    #  self.logger.info("Subscribed to topic")

    # def on_unsubscribe(self, client, userdata, mid, properties, reason_codes):
    #  self.logger.info("Unsubscribed from topic")

    def message_callback_add(self, topic: str, callback: callable):
        """
        Add a callback to a topic. When a message is received on the topic, the callback will be called.
        The callback should take the form of a function that accepts three arguments: client, userdata, message.
        callback(client, userdata, message)
        :topic: str - The topic to add the callback to
        :callback: function - The function to be called
        """
        if not callable(callback):
            raise NodeError("Callback must be a function")
        if not isinstance(topic, str):
            raise NodeError("Topic must be a string")
        if not topic in self.subscriptions:
            self.logger.warning(
                f"Topic {topic} not in subscriptions. Adding topic to subscriptions.",
                extra={
                    "topic": topic,
                },
            )
            self.subscribe(topic)
        self.client.message_callback_add(topic, callback)
        # logger.info(
        #     f"Added callback to topic: {topic}",
        #     extra={"topic": topic, "callback": callback.__name__},
        # )

    def on_log(self, client, userdata, level, buf):
        self.logger.debug("Log: {}".format(buf))

    def _get_id(self):
        # Return a unique id for each node
        return f"{self.node_type}_{time.time_ns()}"

    def _send_latency_request(self):
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

    def _send_latency_response(self, client, userdata, message):
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
        try:
            self.stop_periodic_latency_check()
            self.client.disconnect()
            self.logger.info(f"Disconnected from broker at {self.hostname}:{self.port}")
        except AttributeError:
            # Nothing to disconnect
            pass
