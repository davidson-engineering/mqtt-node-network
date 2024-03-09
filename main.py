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
from mqtt_node_network.metrics_gatherer import MQTTMetricsGatherer
from mqtt_node_network.configuration import broker_config, logger_config


def setup_logging(logger_config):
    from pathlib import Path

    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)


def publish_forever():
    client = (
        MQTTNode(broker_config=broker_config, node_id="node_0").connect().loop_start()
    )

    while True:
        data = random.random()
        payload = json.dumps(data)
        client.publish(topic="pzero/sensorbox_lower/temperature/IR/0", payload=payload)
        time.sleep(1)


def subscribe_forever():
    buffer = []
    client = (
        MQTTMetricsGatherer(
            broker_config=broker_config, node_id="client_0", buffer=buffer
        )
        .connect()
        .loop_start()
    )
    client.subscribe(topic="pzero/#", qos=0)


if __name__ == "__main__":
    setup_logging(logger_config)
    publish_forever()
    subscribe_forever()

#
