import time
import random
import threading
import logging

from mqtt_node_network.configuration import initialize_logging
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.metrics_node import MQTTMetricsNode

logger = initialize_logging("./config/logging.yaml")


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""

    client = MQTTNode.from_config_file(
        name="publisher", config_file="config/config.toml"
    ).connect()

    def get_random_temperature():
        return random.randint(0, 100)

    client.publish_every(
        topic=f"{client.node_id}/temperature/IR/0",
        payload_func=get_random_temperature,
        interval=1,
    )


def subscribe_forever():
    """Subscribe to the broker and print messages to the console."""
    client = MQTTNode.from_config_file(
        name="subscriber",
        config_file="config/config.toml",
    ).connect()

    def process_message_callback(client, userdata, message):
        print(f"Received temperature data on topic {message.topic}: {message.payload}")

    topic = f"+/temperature/IR/0"
    client.message_callback_add(topic, process_message_callback)

    client.loop_forever()


def publisher_subscriber_threaded():
    """Publish and subscribe in separate threads."""
    publish_thread = threading.Thread(target=publish_forever)
    publish_thread.start()

    subscribe_thread = threading.Thread(target=subscribe_forever)
    subscribe_thread.start()

    publish_thread.join()
    subscribe_thread.join()


def create_node():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    client = MQTTNode.from_config_file(
        name=None, config_file="config/config.toml"
    ).connect()

    while True:
        time.sleep(1)


def create_node_swarm(num_nodes):
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    threads = []
    for _ in range(num_nodes):
        thread = threading.Thread(target=create_node)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    publisher_subscriber_threaded()
    # create_node_swarm(10)
