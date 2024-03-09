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
from mqtt_node_network.configure import broker_config, logger_config

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
<<<<<<< HEAD
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

=======
    client = (
        MQTTNode(broker_config=broker_config, node_id="node_0").connect().loop_start()
    )

    while True:
        data = random.random()
        payload = json.dumps(data)
        client.publish(topic="pzero/sensorbox_lower/temperature/IR/0", payload=payload)
        time.sleep(1)
>>>>>>> 3d69d0147c1155d21a3282fab80ce3c5c5f6d274


def subscribe_forever():
    buffer = []
<<<<<<< HEAD
    client = MQTTClient(
        broker_config=broker_config, node_id=SUBSCRIBER_NODE_ID, buffer=buffer
    ).connect()
    client.subscribe(topics=SUBSCRIBE_TOPICS, qos=QOS)
    client.loop_forever()
=======
    client = (
        MQTTMetricsGatherer(
            broker_config=broker_config, node_id="client_0", buffer=buffer
        )
        .connect()
        .loop_start()
    )
    client.subscribe(topic="pzero/#", qos=0)

>>>>>>> 3d69d0147c1155d21a3282fab80ce3c5c5f6d274


if __name__ == "__main__":
    setup_logging(logger_config)
    publish_forever()
    subscribe_forever()

#
