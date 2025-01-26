import time
import pytest

from mqtt_node_network.configuration import MQTTBrokerConfig

TEST_NODE_NAME = "test_node_987123"

# Public Broker, No Authentication
# @pytest.fixture(scope="session")
# def broker_config():
#     return MQTTBrokerConfig(
#         username="",
#         password="",
#         keepalive=60,
#         hostname="test.mosquitto.org",
#         port=1883,
#         timeout=1,
#         reconnect_attempts=5,
#         clean_session=1,
#     )


# @pytest.fixture(scope="session")
# def broker_config():
#     return MQTTBrokerConfig(
#         username="rw",
#         password="readwrite",
#         keepalive=60,
#         hostname="test.mosquitto.org",
#         port=1884,
#         timeout=1,
#         reconnect_attempts=5,
#         clean_session=1,
#     )


@pytest.fixture(scope="session")
def broker_config():
    return MQTTBrokerConfig(
        username="",
        password="",
        keepalive=60,
        hostname="localhost",
        port=1883,
        timeout=1,
        reconnect_attempts=5,
        clean_session=1,
    )


@pytest.fixture(scope="function")
def mqtt_test_client(broker_config):
    from mqtt_node_network.node import MQTTNode

    client = MQTTNode.from_config_file(
        config_file="tests/config-test.toml",
        # secrets_file="tests/test.env",
        name=TEST_NODE_NAME,
        broker_config=broker_config,
        node_id="",
    )
    client.connect()

    yield client
