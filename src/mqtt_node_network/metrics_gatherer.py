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
    topic_parts = topic.split("/")
    structure_parts = structure.split("/")

    len_field = len(topic_parts) - len(structure_parts) + 1

    if len_field < 0:
        message = f"Metric not processed. Topic is too short for the given structure"
        extra = {"topic": topic, "structure": structure}
        logger.error(message, extra=extra)
        raise ValueError(f"{message}; {extra}")

    other_parts = topic_parts[: len_field + 1]
    field_parts = topic_parts[-len_field:]

    parsed_dict = dict(zip(structure_parts, other_parts))
    parsed_dict["field"] = "_".join(field_parts)

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
        data = message.payload.decode()
        if data == "nan":
            logger.debug(
                f"Null message ignored. Received 'nan' on topic '{message.topic}'"
            )
            return
        data = json.loads(data)
        metric = parse_payload_to_metric(
            value=data, topic=message.topic, structure=TOPIC_STRUCTURE
        )
        metric = self.datatype(**metric)
        self.buffer.append(metric)
