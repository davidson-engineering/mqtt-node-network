from collections import deque
from dataclasses import asdict, dataclass, field
import json
from typing import Mapping, Union
import time
import logging

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.configuration import MQTTBrokerConfig, config

TOPIC_STRUCTURE = config["mqtt"]["node_network"]["topic_structure"]

logger = logging.getLogger(__name__)


@dataclass
class Metric(Mapping):
    measurement: str
    fields: dict
    time: float | int
    tags: dict = field(default_factory=dict)

    def __iter__(self):
        return iter(asdict(self).keys())

    def __getitem__(self, key):
        return asdict(self)[key]

    def __len__(self):
        return len(asdict(self))


def parse_topic(topic: str, structure: str) -> tuple[dict, str]:
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
    parsed_dict[structure_parts[-1]] = "_".join(field_parts)

    return parsed_dict


def parse_payload_to_metric(
    value: Union[int, float, str], topic: str, structure: str
) -> Metric:
    parsed_topic = parse_topic(topic, structure)
    measurement = parsed_topic.pop("measurement")
    fields = {parsed_topic.pop("field"): value}
    metric_time = time.time()
    tags = parsed_topic
    return Metric(measurement=measurement, fields=fields, time=metric_time, tags=tags)


class MQTTMetricsGatherer(MQTTNode):
    def __init__(
        self,
        broker_config: MQTTBrokerConfig,
        buffer: Union[list, deque] = None,
        name=None,
        node_id="",
        node_type=None,
        logger=None,
        datatype: type = Metric,
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

    def on_message(self, client, userdata, message):
        super().on_message(client, userdata, message)
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
            value=data, topic=message.topic, structure=TOPIC_STRUCTURE
        )
        metric = self.datatype(**metric)
        self.buffer.append(metric)
