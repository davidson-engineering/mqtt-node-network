#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

import json
import time
import logging

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import broker_config, config
from sensor_library.dht import SensorDHT11

logger = logging.getLogger(__name__)

NODE_ID = "metrics_client_0"
PROMETHEUS_ENABLE = config["mqtt"]["node_network"]["enable_prometheus_server"]
PROMETHEUS_PORT = config["mqtt"]["node_network"]["prometheus_port"]
SENSOR_PIN = 18
GATHER_PERIOD = config["mqtt"]["node_network"]["gather_period"]


def gather_data():
    client = MQTTNode(broker_config=broker_config, node_id=NODE_ID).connect()
    sensor = SensorDHT11(pin=SENSOR_PIN)

    while True:
        data = {
            "measurement": "workshop",
            "fields": sensor.read(),
            "time": time.time(),
            "tags": {"node_id": client.node_id},
        }
        payload = json.dumps(data)
        client.publish(topic=f"{client.node_id}/metric", payload=payload)
        time.sleep(GATHER_PERIOD)


if __name__ == "__main__":
    gather_data()
