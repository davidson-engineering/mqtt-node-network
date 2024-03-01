#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

# import context  # Ensures paho is in PYTHONPATH
import json
import threading
import time
import random
from logging.config import dictConfig
import asyncio

from mqtt_sensor.node import MQTTNode
from mqtt_sensor.metrics_gatherer import MQTTMetricsGatherer
from mqtt_sensor.configuration import broker_config, logger_config

def setup_logging(logger_config):
    from pathlib import Path
    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)

def publish_forever():
    client = MQTTNode(broker_config=broker_config, node_id="node_0").connect()

    while True:
        data = {
            "measurement":"test_measure",
            "fields":{"random_data": random.random()},
            "time":time.time()
        }
        payload = json.dumps(data)
        client.publish(topic="node_0/metrics", payload=payload)
        time.sleep(0.2)

def subscribe_forever():
    buffer = []
    client = MQTTMetricsGatherer(broker_config=broker_config, node_id="client_0", buffer=buffer).connect()
    client.subscribe(topic="node_0/metrics", qos=0)
    client.loop_forever()

if __name__ == "__main__":
    setup_logging(logger_config)
    subscribe_forever()
    # publish_forever()
# 