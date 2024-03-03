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

import json
import threading
import time
import random
from logging.config import dictConfig

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import MQTTClient
from mqtt_node_network.configuration import broker_config, logger_config

SUBSCRIBER_NODE_ID = "node_subscriber_0"
PUBLISHER_NODE_ID = "node_publisher_0"
GATHER_PERIOD = 1
SUBSCRIBE_TOPICS = ["+/metrics"]
PUBLISH_TOPIC = f"{PUBLISHER_NODE_ID}/metrics"
QOS = 0


def setup_logging(logger_config):
    from pathlib import Path

    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)


def publish_forever():
    client = MQTTNode(broker_config=broker_config, node_id=PUBLISHER_NODE_ID).connect()

    while True:
        data = {
            "measurement": "test_measure",
            "fields": {"random_data": random.random()},
            "time": time.time(),
        }
        payload = json.dumps(data)
        client.publish(topic=PUBLISH_TOPIC, payload=payload)
        time.sleep(GATHER_PERIOD)


def subscribe_forever():
    buffer = []
    client = MQTTClient(
        broker_config=broker_config, node_id=SUBSCRIBER_NODE_ID, buffer=buffer
    ).connect()
    client.subscribe(topics=SUBSCRIBE_TOPICS, qos=QOS)
    client.loop_forever()


if __name__ == "__main__":
    setup_logging(logger_config)
    threading.Thread(target=publish_forever).start()
    threading.Thread(target=subscribe_forever).start()

#
