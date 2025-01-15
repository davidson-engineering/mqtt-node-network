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
import threading
from typing import NoReturn, Optional
import time
import copy

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from prometheus_client import Gauge

from mqtt_node_network.configuration import LatencyMonitoringConfig
from mqtt_node_network.node import MQTTNode, dict_to_user_properties


# Initialize your logger and adapter
logger = logging.getLogger(__name__)


class LatencyNode(MQTTNode):
    """
    A subclass of MQTTNode that includes latency monitoring functionality.
    """

    node_client_to_client_latency = Gauge(
        "node_client_to_client_latency",
        "Estimated latency between two clients connected through the MQTT broker",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    def __init__(
        self,
        broker_config: MQTTBrokerConfig,  # type: ignore
        name: str,
        node_id: Optional[str] = None,
        subscribe_config: SubscribeConfig = None,  # type: ignore
        latency_config: LatencyMonitoringConfig = None,  # type: ignore
        properties: dict[str, MQTTPacketProperties] = None,  # type: ignore
    ):
        """
        Initialize an MQTTNode instance.

        :param broker_config: The configuration for the MQTT broker.
        :param name: The name of the node.
        :param node_id: A unique identifier for the node (optional).
        :param subscribe_config: Configuration for subscribed topics.
        :param latency_config: Configuration for latency monitoring.
        """
        super().__init__(
            broker_config=broker_config,
            name=name,
            node_id=node_id,
            subscribe_config=subscribe_config,
            properties=properties,
        )
        # Set latency metrics
        self._latency_thread = None
        self._stop_event = threading.Event()
        self.latency_config = copy.deepcopy(latency_config) or LatencyMonitoringConfig()
        self.latency_config.response_topic = (
            f"{self.client_id}/{self.latency_config.response_topic}"
        )
        self.latency_config.request_topic = (
            f"{self.client_id}/{self.latency_config.request_topic}"
        )
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
        except AttributeError:
            # Nothing to disconnect
            pass
        super().__del__()
