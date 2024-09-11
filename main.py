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
PUBLISH_TOPIC = config["mqtt"]["node_network"]["publish_topic"]
SUBSCRIBE_TOPICS = config["mqtt"]["client"]["subscribe_topics"]
QOS = config["mqtt"]["client"]["subscribe_qos"]
PUBLISH_PERIOD = config["mqtt"]["node_network"]["publish_period"]


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode(broker_config=BROKER_CONFIG).connect()

    while True:
        payload = random.random()
        client.publish(
            topic=f"{client.node_id}/{PUBLISH_TOPIC}/temperature/IR/0", payload=payload
        )
        time.sleep(PUBLISH_PERIOD)


def subscribe_forever():
    """Subscribe to the broker and print messages to the console."""
    buffer = []
    client = MQTTClient(
        broker_config=BROKER_CONFIG,
        buffer=buffer,
        topic_structure=config["mqtt"]["node_network"]["topic_structure"],
    ).connect()
    client.subscribe(topic=SUBSCRIBE_TOPICS, qos=QOS)
    while True:
        time.sleep(1)


if __name__ == "__main__":
    publish_forever()
    # and/or
    subscribe_forever()

#
