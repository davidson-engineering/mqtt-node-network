from collections import deque
from dataclasses import asdict, dataclass, field
import json
from typing import Union
from collections.abc import MutableMapping
import time
import logging
from prometheus_client import Counter

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.node import MQTTBrokerConfig


logger = logging.getLogger(__name__)


@dataclass
class Metric(MutableMapping):
    measurement: str
    fields: dict
    time: float | int
    tags: dict = field(default_factory=dict)

    def __getitem__(self, key):
        return asdict(self)[key]

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __delitem__(self, key):
        if hasattr(self, key):
            setattr(
                self, key, None
            )  # or handle differently if setting to None is inappropriate
        else:
            raise KeyError(f"{key} is not a valid attribute of Metric")

    def __iter__(self):
        return iter(asdict(self))

    def __len__(self):
        return len(asdict(self))


def parse_topic(
    topic: str, structure: str, field_separator: str = "-"
) -> tuple[dict, str]:
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
) -> Metric:
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


class MQTTClient(MQTTNode):

    client_bytes_received_count = Counter(
        "client_bytes_received_total",
        "Total number of bytes received by a client node",
        labelnames=("measurement", "field"),
    )
    client_bytes_sent_count = Counter(
        "client_bytes_sent_total",
        "Total number of bytes sent by a client node",
        labelnames=("measurement", "field"),
    )

    client_messages_received_count = Counter(
        "client_messages_received_total",
        "Total number of messages received by a client node",
        labelnames=("measurement", "field"),
    )

    client_messages_sent_count = Counter(
        "client_messages_sent_total",
        "Total number of messages sent by a client node",
        labelnames=("measurement", "field"),
    )

    def __init__(
        self,
        broker_config: MQTTBrokerConfig,
        topic_structure: str,
        buffer: Union[list, deque] = None,
        name=None,
        node_id="",
        node_type=None,
        logger=None,
        datatype: type = dict,
    ):
        super().__init__(
            broker_config,
            name=name,
            node_id=node_id,
            node_type=node_type,
            logger=logger,
        )

        if buffer is None:
            buffer = []
        self.buffer = buffer
        self.datatype = datatype
        self.topic_structure = topic_structure

    def on_message(self, client, userdata, message):
        super().on_message(client, userdata, message)

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
                self.client_messages_received_count.labels(
                    measurement=metric["measurement"],
                    field=metric_field,
                ).inc()
                self.client_bytes_received_count.labels(
                    measurement=metric["measurement"],
                    field=metric_field,
                ).inc(len(message.payload))

            if not isinstance(metric, self.datatype):
                metric = self.datatype(**metric)
            self.buffer.append(metric)
