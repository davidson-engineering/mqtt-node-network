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
import time
import random
from logging.config import dictConfig
import asyncio

from mqtt_sensor.mqtt import MQTTNode
from mqtt_sensor.client import AsyncMQTTNode
from mqtt_sensor.configuration import broker_config, logger_config


def publish():
    publisher = MQTTNode(broker_config=broker_config, node_id="node_0")
    publisher.publish(topic="category", payload=random.random())


def main():
    loop = asyncio.get_event_loop()
    client = AsyncMQTTNode(loop, broker_config=broker_config, node_id="async_client_0")
    client.subscribe(topic="node_0/category")

    loop.run_until_complete(client.main())
    loop.close()


if __name__ == "__main__":
    dictConfig(logger_config)
    # main()
    publish()
