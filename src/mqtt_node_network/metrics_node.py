from __future__ import annotations
from collections import deque
from dataclasses import asdict, dataclass, field
import json
from typing import Dict, List, Optional, Union, Type, Deque
from collections.abc import MutableMapping
import time
import logging
from prometheus_client import Counter

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import (
    MQTTBrokerConfig,
    SubscribeConfig,
    MQTTPacketProperties,
)

logger = logging.getLogger(__name__)


@dataclass
class Metric(MutableMapping):
    measurement: str
    fields: Dict[str, Union[int, float, str]]
    time: Union[float, int]
    tags: Dict[str, str] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Union[str, int, float, dict]:
        return asdict(self)[key]

    def __setitem__(self, key: str, value: Union[str, int, float, dict]) -> None:
        setattr(self, key, value)

    def __delitem__(self, key: str) -> None:
        if hasattr(self, key):
            setattr(
                self, key, None
            )  # or handle differently if setting to None is inappropriate
        else:
            raise KeyError(f"{key} is not a valid attribute of Metric")

    def __iter__(self) -> iter:
        return iter(asdict(self))

    def __len__(self) -> int:
        return len(asdict(self))


def parse_topic(
    topic: str, structure: str, field_separator: str = "-"
) -> Dict[str, str]:
    """
    Parse a topic string into a dictionary based on a given structure.

    Args:
        topic: The topic string to parse.
        structure: The structure template to match against the topic.
        field_separator: Separator for multi-part fields. Defaults to "-".

    Returns:
        A dictionary mapping structure fields to topic parts.
    """
    topic_parts = topic.rstrip("/").split("/")
    structure_parts = structure.rstrip("/").split("/")
    len_diff = len(topic_parts) - len(structure_parts)
    if structure_parts[-1].endswith("*"):
        len_field = len_diff + 1
        structure_parts[-1] = structure_parts[-1][:-1]
    else:
        if len_diff <= 0:
            len_field = 1
        else:
            message = f"Metric not processed. Topic is too long for the given structure"
            extra = {"topic": topic, "structure": structure}
            logger.error(message, extra=extra)
            raise ValueError(f"{message}; {extra}")

    if len_field <= 0 or len_diff < 0:
        message = f"Metric not processed. Topic is too short for the given structure"
        extra = {"topic": topic, "structure": structure}
        logger.error(message, extra=extra)
        raise ValueError(f"{message}; {extra}")

    other_parts = topic_parts[:-len_field]
    field_parts = topic_parts[-len_field:]

    parsed_dict = dict(zip(structure_parts, other_parts))
    parsed_dict[structure_parts[-1]] = field_separator.join(field_parts)

    return parsed_dict


def parse_payload_to_metric(
    value: Union[int, float, str], topic: str, structure: str
) -> Union[Metric, Dict, None]:
    """
    Convert a payload and topic into a Metric object or dictionary.

    Args:
        value: The payload value (e.g., int, float, or string).
        topic: The topic string associated with the value.
        structure: The expected structure of the topic.

    Returns:
        A Metric object or dictionary, or None if parsing fails.
    """
    try:
        parsed_topic = parse_topic(topic, structure)
    except ValueError as e:
        logger.error(
            f"Failed to parse topic: {e}",
            extra={"topic": topic, "structure": structure},
        )
        return None
    measurement = parsed_topic.pop("measurement")
    fields = {parsed_topic.pop("field"): value}
    metric_time = time.time()
    tags = parsed_topic
    return asdict(
        Metric(measurement=measurement, fields=fields, time=metric_time, tags=tags)
    )


class MQTTMetricsNode(MQTTNode):
    """
    A specialized MQTTNode for processing metrics with Prometheus integration.
    """

    metric_bytes_received_count = Counter(
        "metric_bytes_received_total",
        "Total number of bytes received by a metric node",
        labelnames=("measurement", "field"),
    )
    metric_bytes_sent_count = Counter(
        "metric_bytes_sent_total",
        "Total number of bytes sent by a metric node",
        labelnames=("measurement", "field"),
    )

    metric_messages_received_count = Counter(
        "metric_messages_received_total",
        "Total number of messages received by a metric node",
        labelnames=("measurement", "field"),
    )

    metric_messages_sent_count = Counter(
        "metric_messages_sent_total",
        "Total number of messages sent by a metric node",
        labelnames=("measurement", "field"),
    )

    def __init__(
        self,
        name: str,
        broker_config: MQTTBrokerConfig,
        topic_structure: str,
        node_id: Optional[str] = None,
        buffer: Optional[Union[List, Deque]] = None,
        subscribe_config: Optional[SubscribeConfig] = None,
        datatype: Optional[Type] = dict,
        packet_properties: dict[str, MQTTPacketProperties] = None,
    ):
        """
        Initialize the MQTTMetricsNode.

        Args:
            name: The name of the node.
            broker_config: Configuration for the MQTT broker.
            topic_structure: The expected structure of topics.
            node_id: An optional unique identifier for the node.
            buffer: An optional buffer for storing parsed metrics (e.g., a list or deque).
            subscribe_config: Configuration for subscription topics.
            latency_config: Configuration for latency monitoring.
            datatype: The expected type for parsed metrics. Defaults to dict.
        """
        super().__init__(
            broker_config,
            name=name,
            node_id=node_id,
            subscribe_config=subscribe_config,
            packet_properties=packet_properties,
        )

        self.buffer = buffer if buffer else deque()
        self.datatype = datatype
        self.topic_structure = topic_structure

    def on_message(self, metric, userdata, message):
        """
        Handle incoming MQTT messages, parse them into metrics, and store in the buffer.

        Args:
            metric: The metric to process.
            userdata: User-specific data passed during message receipt.
            message: The MQTT message object.
        """
        super().on_message(metric, userdata, message)

        data = message.payload.decode()
        if message.payload is None:
            logger.debug(
                f"Null message ignored. Received None on topic '{message.topic}'"
            )
            return

        elif isinstance(message.payload, bytes):
            data = message.payload.decode()

        if data == "nan":
            logger.debug(
                f"Null message ignored. Received 'nan' on topic '{message.topic}'"
            )
            return

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                logger.debug("Message is not JSON. Attempting to parse as a string")
                pass

        if not isinstance(data, (str, int, float)):
            logger.error(
                f"Message is not a valid type. Received '{type(data)}' on topic '{message.topic}'"
            )
            return

        metric = parse_payload_to_metric(
            value=data, topic=message.topic, structure=self.topic_structure
        )
        if metric:
            for metric_field in metric["fields"].keys():
                self.metric_messages_received_count.labels(
                    measurement=metric["measurement"],
                    field=metric_field,
                ).inc()
                self.metric_bytes_received_count.labels(
                    measurement=metric["measurement"],
                    field=metric_field,
                ).inc(len(message.payload))

            if not isinstance(metric, self.datatype):
                metric = self.datatype(**metric)
            self.buffer.append(metric)
