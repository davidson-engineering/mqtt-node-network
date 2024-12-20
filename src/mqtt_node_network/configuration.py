from dataclasses import asdict, dataclass
from logging.config import dictConfig
from pathlib import Path
from typing import Mapping, Optional, Union
import logging
from config_loader import load_configs


class UnpackMixin(Mapping):
    def __iter__(self):
        return iter(asdict(self).keys())

    def __len__(self):
        return len(asdict(self))

    def __getitem__(self, key):
        if key not in asdict(self):
            raise KeyError(f"Key {key} not found in {self.__class__.__name__}")
        return getattr(self, key)


@dataclass
class MQTTBrokerConfig:
    username: str
    password: str
    keepalive: int
    hostname: str
    port: int
    timeout: int
    reconnect_attempts: int


@dataclass
class LatencyMonitoringConfig:
    enabled: bool = False  # Whether to enable latency monitoring
    request_topic: str = "request"
    response_topic: str = "response"
    qos: int = 1  # MQTT Quality of Service level for latency monitoring
    interval: int = 10  # How often to check latency (in seconds)
    log_enabled: bool = False


@dataclass
class SubscribeConfig:
    topics: list
    qos: int


@dataclass
class MQTTNodeConfig(UnpackMixin):
    name: str
    broker_config: MQTTBrokerConfig
    node_id: str = None
    subscribe_config: SubscribeConfig = None
    latency_config: LatencyMonitoringConfig = None


@dataclass
class MQTTMetricsNodeConfig(UnpackMixin):
    topic_structure: str
    datatype: type = dict


def get_nested_value(config, target_key):
    # Base case: if the target key is in the current dictionary, return its value
    if target_key in config:
        return config[target_key]

    # Recursive case: check for the target key in any nested dictionaries
    for key, value in config.items():
        if isinstance(value, dict):
            result = get_nested_value(value, target_key)
            if result is not None:
                return result

    # Return None if the target key is not found
    return None


def initialize_logging(logging_config: Union[dict, str]) -> logging.Logger:
    """
    Initialize the logger.

    Args:
    - logging_config: The logging configuration dictionary or file path.

    """
    if isinstance(logging_config, str):
        logging_config = load_configs(logging_config)
    dictConfig(logging_config)
    return logging.getLogger("mqtt_node_network")


def initialize_config(
    config: str | Path,
    secrets: Optional[str | Path] = None,
) -> dict:
    """
    Initialize the configuration and logger.

    Args:
    - config: The configuration file path or list of configuration file paths.
    - secrets: The secrets file path. Will default to ".env" if not provided.

    """
    config = load_configs(config, secrets_filepath=secrets)
    config = get_nested_value(config, "mqtt")

    broker_config = MQTTBrokerConfig(
        username=config["broker"].get("username", "mqtt"),
        password=config["broker"].get("password", ""),
        hostname=config["broker"].get("hostname", "localhost"),
        port=config["broker"].get("port", 1883),
        keepalive=config["broker"].get("keepalive", 60),
        timeout=config["broker"].get("timeout", 5),
        reconnect_attempts=config["broker"].get("reconnect_attempts", 10),
    )
    latency_config = LatencyMonitoringConfig(**config["node"]["metrics"].get("latency"))
    subscribe_config = SubscribeConfig(
        topics=config["subscriptions"]["subscribe_topics"],
        qos=config["subscriptions"]["subscribe_qos"],
    )
    metrics_node_config = MQTTMetricsNodeConfig(
        topic_structure=config["metrics_node"]["topic_structure"],
    )
    node_config = MQTTNodeConfig(
        name=config["node"]["name"],
        broker_config=broker_config,
        node_id=config["node"].get("node_id", None),
        subscribe_config=subscribe_config,
        latency_config=latency_config,
    )

    # merge metrics_node_config and node_config
    metrics_node_config = {**dict(node_config), **dict(metrics_node_config)}

    return dict(
        MQTTNode=node_config,
        MQTTMetricsNode=metrics_node_config,
    )
