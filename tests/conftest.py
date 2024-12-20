import time
import pytest


@pytest.fixture(scope="session")
def mqtt_client():
    from mqtt_node_network.configuration import initialize_config
    from mqtt_node_network.node import MQTTNode

    client = MQTTNode.from_config_file(
        config_file="tests/config-test.toml", secrets_file="tests/test.env"
    )
    client.connect()

    timeout = 10

    while not client.is_connected() or timeout < 0:
        time.sleep(1)
        timeout -= 1
    if timeout < 0:
        raise TimeoutError("Client connection timeout")
    yield client
