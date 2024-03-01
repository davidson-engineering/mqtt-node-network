#!/usr/bin/env python3

import asyncio
import socket
import context  # Ensures paho is in PYTHONPATH

import paho.mqtt.client as mqtt
from mqtt_sensor.mqtt import MQTTNode


class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        print("Socket opened")

        def cb():
            print("Socket is readable, calling loop_read")
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        print("Socket closed")
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        print("Watching socket for writability.")

        def cb():
            print("Socket is writable, calling loop_write")
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        print("Stop watching socket for writability.")
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        print("misc_loop started")
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
        print("misc_loop finished")


class AsyncMQTTNode(MQTTNode):
    def __init__(
        self, loop, broker_config, name=None, node_id="", node_type=None, logger=None
    ):
        super().__init__(broker_config, name, node_id, node_type)
        self.loop = loop
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.node_id
        )
        self.client.username_pw_set(self._username, self._password)
        if logger:
            self.client.enable_logger(logger)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, reason_code, properties):
        print("Subscribing")
        # client.subscribe(topic)

    def subscribe(self, topic="#", qos=0):
        return self.client.subscribe(topic, qos)

    def on_message(self, client, userdata, msg):
        if not self.got_message:
            print("Got unexpected message: {}".format(msg.decode()))
        else:
            self.got_message.set_result(msg.payload)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.disconnected.set_result(reason_code)

    async def main(self):
        self.disconnected = self.loop.create_future()
        self.got_message = None

        aioh = AsyncioHelper(self.loop, self.client)
        self.client.connect(self.hostname, self.port, self.keepalive)
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        try:
            while True:
                self.got_message = self.loop.create_future()
                msg = await self.got_message
                print("Got response with {} bytes".format(len(msg)))
                self.got_message = None
        finally:
            self.client.disconnect()
            print("Disconnected: {}".format(await self.disconnected))


# print("Starting")
# loop = asyncio.get_event_loop()
# loop.run_until_complete(AsyncMqttExample(loop).main())
# loop.close()
# print("Finished")
