# MQTT Node Network

`mqtt_node_network` is a Python package designed for interacting with MQTT brokers, offering a simple way to handle nodes and clients in a publish-subscribe messaging model. The package supports metrics collection, topic parsing, and Prometheus integration for monitoring message and byte transfers.

## Features

- **MQTTNode Class**: The base class for connecting to an MQTT broker, allowing the creation of nodes for publishing and subscribing.
- **MQTTClient Class**: An extension of `MQTTNode` that includes additional functionality for message buffering, topic parsing, and metrics collection.
- **Prometheus Metrics**: Integrated counters for tracking MQTT messages and byte traffic.
- **Flexible Topic Parsing**: Define custom topic structures for extracting relevant information from MQTT topics.

## Installation

To install application and required dependencies for `mqtt_node_network`, run:

```
>>> git clone https://github.com/davidson-engineering/mqtt-node-network.git
>>> pip install .
```

## Usage

### 1. MQTTNode

The `MQTTNode` class allows you to establish a connection with an MQTT broker. This node can act as a base for creating more complex functionality like publishing and subscribing.

```python
from mqtt_node_network.node import MQTTNode, MQTTBrokerConfig

# Example broker configuration
broker_config = MQTTBrokerConfig(
    host="broker.hivemq.com",
    port=1883,
    keepalive=60,
)

# Initialize an MQTTNode
mqtt_node = MQTTNode(broker_config=broker_config)

# Connect to the broker
mqtt_node.connect()

# Publish data
mqtt_node.publish(topic="sensor/office/temperature", payload="22.5")
```

### 2. MQTTClient

The `MQTTClient` class extends `MQTTNode` by adding functionality to parse incoming payloads and store them in a buffer. It also tracks message metrics using Prometheus counters.

```python
from mqtt_node_network.client import MQTTClient

# Define a topic structure for parsing
topic_structure = "sensor/location/device/measurement"

# Initialize the MQTT client with buffer
mqtt_client = MQTTClient(
    broker_config=broker_config,
    topic_structure=topic_structure,
    buffer=[]
)

# Connect to the broker
mqtt_client.connect()

# Subscribe to topics
mqtt_client.subscribe(topic="sensor/#")

# Handle messages (automatically increments Prometheus counters)
mqtt_client.loop_forever()
```

### 3. Topic Parsing

The `parse_topic` function allows for extracting structured data from MQTT topics. You can customize the structure to match your specific use case.

```python
from mqtt_node_network.client import parse_topic

topic = "sensor/office/device123/temperature"
structure = "sensor/location/device/measurement"

parsed = parse_topic(topic, structure)

print(parsed)
# Output: {'sensor': 'sensor', 'location': 'office', 'device': 'device123', 'measurement': 'temperature'}
```

### 4. Metrics Collection

The `MQTTClient` automatically collects metrics using Prometheus counters. These metrics include:

- **Bytes Received**: `client_bytes_received_total`
- **Bytes Sent**: `client_bytes_sent_total`
- **Messages Received**: `client_messages_received_total`
- **Messages Sent**: `client_messages_sent_total`

You can view and track these metrics using Prometheus or Grafana for monitoring system performance.

```python
from prometheus_client import start_http_server

# Start a Prometheus metrics server
start_http_server(8000)

# Run the MQTT client
mqtt_client.loop_forever()
```

### 5. Buffering Metrics

Incoming messages are parsed into `Metric` objects and stored in a buffer for further processing. You can customize the buffer to store different types of data structures (e.g., a list or deque).

```python
# Access the buffer after receiving messages
for metric in mqtt_client.buffer:
    print(metric)
```

## Configuration

The MQTT client can be configured using a configuration file in formats like `TOML` or `YAML`. The `initialize_config` function is used to initialize the application configuration. These configurations include broker settings, QoS levels, and topics to subscribe to or publish to.

Environment variables are automatically parsed in the  configuration file if the format ${MY_ENV_VAR} is used.

```python
from mqtt_node_network.initialize import initialize_config

# Initialize the configuration
config = initialize_config(config="config/config.toml")
```


Example `config.toml`:

```toml
[broker]
username = "${MQTT_BROKER_USERNAME}"
password = "${MQTT_BROKER_PASSWORD}"
host = "broker.hivemq.com"
port = 1883
keepalive = 60

[client]
subscribe_topics = ["sensor/#"]
publish_topic = "sensor/office/temperature"
publish_period = 5
subscribe_qos = 1

[node_network]
topic_structure = "sensor/location/device/measurement"
```


### Example Logging Configuration (`logging.yaml`)


You can customize the logging behavior using a `logging.yaml` file to manage log levels, handlers, and formats for different components. The location of this file is passed into the `initialize_config` function using the `logging_config` argument. 
For more information on using dictConfig, see [here](https://docs.python.org/3/library/logging.config.html).

```python
config = initialize_config(config="config/config.toml", logging_config="config/logging.yaml")
```
#### Example logging yaml file:
```yaml
version: 1
formatters:
  simple:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

handlers:
  console:
    class: logging.StreamHandler
    formatter: simple
    level: DEBUG

loggers:
  mqtt_node_network:
    level: DEBUG
    handlers: [console]
    propagate: no

root:
  level: INFO
  handlers: [console]
```
