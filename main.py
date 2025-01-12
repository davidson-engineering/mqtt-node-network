import sys
import time
import random
import threading
import logging

from mqtt_node_network.configuration import initialize_logging
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.metrics_node import MQTTMetricsNode

logger = initialize_logging("./config/logging.yaml")


# def publish_forever():
#     """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""

#     client = MQTTNode.from_config_file(
#         name="publisher", config_file="config/config.toml"
#     ).connect()

#     def get_random_temperature():
#         return random.randint(0, 100)

#     client.publish_every(
#         topic=f"{client.node_id}/temperature/IR/0",
#         payload_func=get_random_temperature,
#         interval=1,
#     )


# def subscribe_forever():
#     """Subscribe to the broker and print messages to the console."""
#     client = MQTTNode.from_config_file(
#         name="subscriber",
#         config_file="config/config.toml",
#     ).connect()

#     def process_message_callback(client, userdata, message):
#         print(f"Received temperature data on topic {message.topic}: {message.payload}")

#     topic = f"+/temperature/IR/0"
#     client.message_callback_add(topic, process_message_callback)
#     client.loop_forever(timeout=10)


def publish_forever():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""

    publisher = MQTTNode.from_config_file(
        name="publisher", config_file="config/config.toml"
    ).connect()

    def get_random_temperature():
        return random.randint(0, 100)

    # """Subscribe to the broker and print messages to the console."""
    # subscriber = MQTTNode.from_config_file(
    #     name="subscriber",
    #     config_file="config/config.toml",
    # ).connect()

    # def process_message_callback(client, userdata, message):
    #     print(
    #         f"Received temperature data on topic {message.topic}: {message.payload.decode()}"
    #     )

    # topic = f"+/temperature/IR/0"
    # # subscriber.message_callback_add(topic, process_message_callback)

    # Blocking call that publishes every second
    publisher.publish_every(
        topic=f"{publisher.node_id}/temperature/IR/0",
        payload_func=get_random_temperature,
        interval=1,
    )


def create_loop_forever_node():
    """Publish random temperature data to the broker every PUBLISH_PERIOD seconds."""
    listener = MQTTNode.from_config_file(
        name=None, config_file="config/config.toml"
    ).connect()

    def process_message_callback(client, userdata, message):
        print(
            f"Received temperature data on topic {message.topic}: {message.payload.decode()}"
        )

    topic = f"+/temperature/IR/0"
    listener.message_callback_add(topic, process_message_callback)

    listener.loop_forever(timeout=10)
    # while True:
    #     time.sleep(1)


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


def simple_create_node(config_file) -> MQTTNode:
    node = MQTTNode.from_config_file(name=None, config_file=config_file)
    response = node.connect()
    if not response:
        return node


if __name__ == "__main__":
    try:
        node = simple_create_node("config/config.toml")
        # publish_forever()
        # create_loop_forever_node()
        # publisher_subscriber_threaded()
        # create_node_swarm(10)
        node.loop_forever(timeout=10)
    except Exception as e:
        logger.error(f"Error: {e}")
    except KeyboardInterrupt:
        logger.info("Exiting")
        sys.exit(0)
