#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------

import logging

from mqtt_node_network import SubscribeOptions
from mqtt_node_network.metrics_gatherer import MQTTMetricsGatherer
from mqtt_node_network.configuration import (
    broker_config,
    config,
)
from fast_database_clients import FastInfluxDBClient

logger = logging.getLogger(__name__)

NODE_ID = config["mqtt"]["node_network"]["node_id"]
INFLUXDB_CONFIG_FILEPATH = config["influxdb"]["config_filepath"]


def gather_client(topics=("+/metric"), qos=0):
    database_client = FastInfluxDBClient.from_config_file(
        config_file=INFLUXDB_CONFIG_FILEPATH
    )
    database_client.start()

    client = MQTTMetricsGatherer(
        broker_config=broker_config, node_id=NODE_ID, buffer=database_client.buffer
    ).connect()
    topics = tuple(SubscribeOptions(topic=topic, qos=qos) for topic in topics)
    client.subscribe(topics)
    client.loop_forever()


if __name__ == "__main__":

    gather_client()
