from dataclasses import asdict, dataclass
from logging.config import dictConfig
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Union
import logging
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions
from paho.mqtt.client import MQTT_CLEAN_START_FIRST_ONLY
import ssl

from config_loader import load_configs


class UnpackMixin(Mapping):
    """A mixin class to unpack dataclass attributes as a mapping."""

    def __iter__(self):
        return iter(asdict(self).keys())

    def __len__(self):
        return len(asdict(self))

    def __getitem__(self, key):
        if key not in asdict(self):
            raise KeyError(f"Key {key} not found in {self.__class__.__name__}")
        return getattr(self, key)


@dataclass
class MQTTBrokerConfig(UnpackMixin):
    """Configuration for connecting to an MQTT broker."""

    username: str
    password: str
    keepalive: int
    hostname: str
    port: int
    timeout: int
    reconnect_attempts: int
    clean_session: bool = MQTT_CLEAN_START_FIRST_ONLY


class MQTTPacketProperties:
    """Properties for MQTT Packets"""

    _packet_type = None

    # Properties are parsed to an paho.mqtt.properties.Properties object
    def validate_properties(self):
        raise NotImplementedError(
            "validate_properties must be implemented in child class"
        )

    def _build_packet(self):
        return Properties(self._packet_type)


class MQTTConnectProperties(MQTTPacketProperties):
    """Properties for MQTT CONNECT Packet"""

    _packet_type = PacketTypes.CONNECT

    def __init__(self, session_expiry_interval=0):
        self.session_expiry_interval = session_expiry_interval

    def validate_properties(self):
        if self.session_expiry_interval < 0:
            raise ValueError(
                "Session expiry interval must be greater than or equal to 0"
            )

    def build_properties(self):
        properties = self._build_packet()
        properties.SessionExpiryInterval = self.session_expiry_interval

        return properties


class MQTTPublishProperties(MQTTPacketProperties):
    """Properties for MQTT PUBLISH Packet"""

    _packet_type = PacketTypes.PUBLISH

    def __init__(self, message_expiry_interval=0, retain=False):
        self.message_expiry_interval = message_expiry_interval
        # self.retain_flag = retain

    def validate_properties(self):
        if self.message_expiry_interval < 0:
            raise ValueError(
                "Message expiry interval must be greater than or equal to 0"
            )
        # if not isinstance(self.retain_flag, bool):
        #     raise ValueError("Retain flag must be a boolean")

    def build_properties(self):
        properties = self._build_packet()
        properties.MessageExpiryInterval = self.message_expiry_interval
        # properties.Retain = self.retain_flag

        return properties


@dataclass(frozen=True)
class TLSConfig:
    """TLS configuration class
    Source: https://github.com/matteosox/pysparkplug/blob/1e149c4f6624c7ef9c2a4bc586c5bad196727540/src/pysparkplug/_config.py

    Args:
        ca_certs:
            a string path to the Certificate Authority certificate files that
            are to be treated as trusted by this client. If this is the only
            option given then the client will operate in a similar manner to
            a web browser. That is to say it will require the broker to have
            a certificate signed by the Certificate Authorities in ca_certs
            and will communicate using TLS v1.2, but will not attempt any
            form of authentication. This provides basic network encryption
            but may not be sufficient depending on how the broker is
            configured.
        certfile:
            string pointing to the PEM encoded client certificate. If this
            argument is not None then it will be used as client
            information for TLS based authentication. Support for this
            feature is broker dependent. Note that if this file is
            encrypted and needs a password to decrypt it, Python will ask
            for the password at the command line. It is not currently possible
            to define a callback to provide the password.
        keyfile:
            string pointing to the PEM encoded private keys. If this
            argument is not None then it will be used as client
            information for TLS based authentication. Support for this
            feature is broker dependent. Note that if this file is
            encrypted and needs a password to decrypt it, Python will ask
            for the password at the command line. It is not currently possible
            to define a callback to provide the password.
        cert_reqs:
            defines the certificate requirements that the client imposes on the
            broker. By default this is `ssl.CERT_REQUIRED`, which means that
            the broker must provide a certificate. See the ssl pydoc for more
            information on this parameter.
        tls_version:
            specifies the version of the SSL/TLS protocol to be used. By default
            (if the python version supports it) the highest TLS version is
            detected. If unavailable, TLS v1.2 is used. Previous versions
            (all versions beginning with SSL) are possible but not recommended
            due to possible security problems.
        ciphers:
            a string specifying which encryption ciphers are allowable for this
            connection, or `None` to use the defaults. See the ssl pydoc for more
            information.

    Returns:
        a TLSConfig object
    """

    ca_certs: Optional[str] = None
    certfile: Optional[str] = None
    keyfile: Optional[str] = None
    cert_reqs: ssl.VerifyMode = ssl.VerifyMode.CERT_REQUIRED
    tls_version: ssl._SSLMethod = ssl.PROTOCOL_TLS
    ciphers: Optional[str] = None


@dataclass
class LatencyMonitoringConfig:
    """Configuration for latency monitoring."""

    enabled: bool = False  # Whether to enable latency monitoring
    request_topic: str = "request"
    response_topic: str = "response"
    qos: int = 1  # MQTT Quality of Service level for latency monitoring
    interval: int = 10  # How often to check latency (in seconds)
    log_enabled: bool = False


@dataclass
class SubscribeConfig:
    """Configuration for MQTT subscriptions."""

    topics: List[str]
    options: SubscribeOptions = SubscribeOptions()


@dataclass
class MQTTNodeConfig(UnpackMixin):
    """Configuration for an MQTT node."""

    name: str
    broker_config: MQTTBrokerConfig
    node_id: Optional[str] = None
    subscribe_config: Optional[SubscribeConfig] = None
    properties: dict[str, MQTTPacketProperties] = None


@dataclass
class MQTTMetricsNodeConfig(UnpackMixin):
    """Configuration for an MQTT Metrics Node."""

    topic_structure: str
    datatype: type = dict


@dataclass
class MQTTLatencyNodeConfig(UnpackMixin):
    """Configuration for an MQTT Latency Node."""

    latency_config: Optional[LatencyMonitoringConfig] = None


def get_nested_value(config: Dict, target_key: str):
    """
    Retrieve a nested value from a configuration dictionary.

    Args:
        config: The dictionary to search.
        target_key: The key to find.

    Returns:
        The value associated with the target key, or None if not found.
    """
    if target_key in config:
        return config[target_key]

    for key, value in config.items():
        if isinstance(value, dict):
            result = get_nested_value(value, target_key)
            if result is not None:
                return result

    return None


def initialize_logging(logging_config: Union[Dict, str]) -> logging.Logger:
    """
    Initialize the logger.

    Args:
        logging_config: The logging configuration dictionary or file path.

    Returns:
        A logger instance.
    """
    if isinstance(logging_config, str):
        logging_config = load_configs(logging_config)
    dictConfig(logging_config)
    return logging.getLogger("mqtt_node_network")


def initialize_config(
    config: Union[str, Path],
    secrets: Optional[Union[str, Path]] = None,
) -> Dict:
    """
    Initialize the configuration and logger.

    Args:
        config: The configuration file path or list of configuration file paths.
        secrets: The secrets file path. Will default to ".env" if not provided.

    Returns:
        A dictionary containing node configurations.
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
        clean_session=config["broker"].get(
            "clean_session", MQTT_CLEAN_START_FIRST_ONLY
        ),
    )

    properties = {
        PacketTypes.CONNECT: MQTTConnectProperties(
            session_expiry_interval=config["packet_properties"].get(
                "session_expiry_interval", 0
            ),
        ),
        PacketTypes.PUBLISH: MQTTPublishProperties(
            message_expiry_interval=config["packet_properties"].get(
                "message_expiry_interval", 0
            ),
            retain=config["packet_properties"].get("retain", False),
        ),
    }
    subscribe_config = SubscribeConfig(
        topics=config["subscriptions"]["subscribe_topics"],
        options=SubscribeOptions(
            qos=config["subscriptions"].get("qos", 0),
            noLocal=config["subscriptions"].get("no_local", False),
            retainAsPublished=config["subscriptions"].get("retain_as_published", False),
            retainHandling=config["subscriptions"].get("retain_handling", 0),
        ),
    )

    metrics_node_config = MQTTMetricsNodeConfig(
        topic_structure=config["metrics_node"]["topic_structure"],
    )
    latency_node_config = MQTTLatencyNodeConfig(
        latency_config=LatencyMonitoringConfig(
            **config["latency_node"].get("latency", {})
        )
    )

    node_config = MQTTNodeConfig(
        name=config["node"]["name"],
        broker_config=broker_config,
        properties=properties,
        node_id=config["node"].get("node_id", None),
        subscribe_config=subscribe_config,
    )

    metrics_node_config = {**dict(node_config), **dict(metrics_node_config)}
    latency_node_config = {**dict(node_config), **dict(latency_node_config)}

    return {
        "MQTTNode": node_config,
        "MQTTMetricsNode": metrics_node_config,
        "MQTTLatencyNode": latency_node_config,
    }
