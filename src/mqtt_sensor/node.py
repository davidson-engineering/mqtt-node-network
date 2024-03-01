#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------
from __future__ import annotations
import logging
import itertools

from prometheus_client import Counter

logger = logging.getLogger(__name__)


class NodeError(Exception):
    def __init__(self, message):
        self.message = message
        logger.error(self.message)
        super().__init__(self.message)


class Node:
    _ids = itertools.count()

    node_bytes_received_count = Counter(
        "node_bytes_received_total",
        "Total number of bytes received by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )
    node_bytes_sent_count = Counter(
        "node_bytes_sent_total",
        "Total number of bytes sent by node",
        labelnames=("node_id", "node_name", "node_type", "host"),
    )

    def __init__(
        self,
        name=None,
        node_id=None,
        node_type=None,
    ):
        self.name = name
        self.node_id = node_id or self.get_id()
        self.node_type = node_type or self.__class__.__name__

    def get_id(self):
        # Return a unique id for each node
        return f"{self.node_type}_{next(self._ids)}"


class MQTTNode(Node):

    def __init__(self, name=None, node_id=None, node_type=None):
        super().__init__(self, name=name, node_id=node_id, node_type=node_type)
