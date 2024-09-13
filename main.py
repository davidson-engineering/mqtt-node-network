#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# ---------------------------------------------------------------------------
"""
A simple example of how to use the mqtt_node_network package.
"""
# ---------------------------------------------------------------------------

import time
import random
import threading

from mqtt_node_network.initialize import initialize
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import MQTTClient

# Initialize the configuration
# Logger configuration is optional
config = initialize(
    config="config/config.toml", secrets=".env", logger="config/logger.yaml"
)

# Get the broker configuration from the config dictionary
# Assign to variables for easier access
BROKER_CONFIG = config["mqtt"]["broker"]
SUBSCRIBE_TOPICS = config["mqtt"]["client"]["subscribe_topics"]
QOS = config["mqtt"]["client"]["subscribe_qos"]
PUBLISH_PERIOD = 1


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode(
        node_id="publisher",
        name="publisher",
        broker_config=BROKER_CONFIG,
        latency_config=config["mqtt"]["client"]["metrics"]["latency"],
    ).connect()

    while True:
        payload = random.random()
        client.publish(topic=f"{client.node_id}/temperature/IR/0", payload=payload)
        time.sleep(PUBLISH_PERIOD)


def subscribe_forever():
    """Subscribe to the broker and print messages to the console."""
    buffer = []
    client = MQTTClient(
        node_id="subscriber",
        name="subscriber",
        broker_config=BROKER_CONFIG,
        buffer=buffer,
        topic_structure=config["mqtt"]["node_network"]["topic_structure"],
        latency_config=config["mqtt"]["client"]["metrics"]["latency"],
    ).connect()
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    while True:
        time.sleep(1)


def publisher_subscriber_threaded():
    """Publish and subscribe in separate threads."""
    publish_thread = threading.Thread(target=publish_forever)
    subscribe_thread = threading.Thread(target=subscribe_forever)
    publish_thread.start()
    subscribe_thread.start()
    publish_thread.join()
    subscribe_thread.join()


def create_node():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode(
        node_id="publisher",
        name="publisher",
        broker_config=BROKER_CONFIG,
        latency_config=config["mqtt"]["client"]["metrics"]["latency"],
    ).connect()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    create_node()
#
