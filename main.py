#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

import context  # Ensures paho is in PYTHONPATH
import time
import random
import os

from dotenv import load_dotenv
from mqtt_sensor.client import MQTTClient
from mqtt_sensor.configuration import config

load_dotenv("config/.secrets")

MQTT_BROKER_HOSTNAME = config["mqtt"]["broker"]["hostname"]
MQTT_BROKER_PORT = config["mqtt"]["broker"]["port"]
MQTT_BROKER_USERNAME = os.getenv("MQTT_BROKER_USERNAME")
MQTT_BROKER_PASSWORD = os.getenv("MQTT_BROKER_PASSWORD")
MQTT_BROKER_AUTH = {"username": MQTT_BROKER_USERNAME, "password": MQTT_BROKER_PASSWORD}


def main():
    subscriber = MQTTClient(
        hostname="gfyvrdatadash", auth=MQTT_BROKER_AUTH, client_id="client_0"
    )
    subscriber.subscribe(topic="node_0/category")

    publisher = MQTTClient(
        hostname=MQTT_BROKER_HOSTNAME, auth=MQTT_BROKER_AUTH, client_id="node_0"
    )
    publisher.publish(topic="category", payload=random.random())

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
