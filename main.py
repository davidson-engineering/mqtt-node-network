#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""mqtt_node_network

This module contains the MQTTNode class which is a base class for all MQTT nodes.
It is a wrapper around the paho-mqtt client which provides logging, error handling,
and prometheus metrics.
"""
# ---------------------------------------------------------------------------

import time
import random
from mqtt_node_network.initialize import initialize

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import MQTTClient

PUBLISH_PERIOD = 1
SUBSCRIBE_TOPICS = ["+/metrics/#"]
PUBLISH_TOPIC = "metrics"
QOS = 0

config, logger = initialize(
    config="config/config.toml", secrets=".env", logging_config="config/logger.yaml"
)

BROKER_CONFIG = config["mqtt"]["broker"]


def publish_forever():
    client = MQTTNode(broker_config=BROKER_CONFIG).connect()

    while True:
        payload = random.random()
        client.publish(
            topic=f"{client.node_id}/{PUBLISH_TOPIC}/temperature/IR/0", payload=payload
        )
        time.sleep(PUBLISH_PERIOD)


def subscribe_forever():
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
    # publish_forever()
    # or
    subscribe_forever()

#
