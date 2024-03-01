import context  # Ensures paho is in PYTHONPATH
import time
import random

import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe


class MQTTClient:

    def __init__(self, hostname, port=1883, auth=None, client_id="") -> None:
        self.hostname: str = hostname
        self.port: int = port
        self.address = (self.hostname, self.port)

        self.auth: dict = auth
        self.client_id = client_id

    def print_msg(self, client, userdata, message):
        print("%s : %s" % (message.topic, message.payload))

    def subscribe(self, topic="#", qos=0):
        subscribe.callback(
            self.print_msg,
            topic,
            hostname=self.hostname,
            port=self.port,
            auth=self.auth,
            qos=qos,
            client_id=self.client_id,
        )

    def publish(self, topic, payload, qos=0, retain=False):
        if self.client_id:
            topic = f"{self.client_id}/{topic}"
        publish.single(
            payload=payload,
            topic=topic,
            hostname=self.hostname,
            port=self.port,
            auth=self.auth,
            qos=qos,
            retain=retain,
            client_id=self.client_id,
        )
