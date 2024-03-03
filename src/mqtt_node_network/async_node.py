# #!/usr/bin/env python3

# import asyncio
# import socket
# from typing import Union
# import context  # Ensures paho is in PYTHONPATH
# import logging

# import paho.mqtt.client as mqtt
# from mqtt_sensor.node import MQTTNode

# logger = logging.getLogger(__name__)

# class AsyncioHelper:
#     def __init__(self, loop, client):
#         self.loop = loop
#         self.client = client
#         self.client.on_socket_open = self.on_socket_open
#         self.client.on_socket_close = self.on_socket_close
#         self.client.on_socket_register_write = self.on_socket_register_write
#         self.client.on_socket_unregister_write = self.on_socket_unregister_write

#     def on_socket_open(self, client, userdata, sock):
#         print("Socket opened")

#         def cb():
#             logger.debug("Socket is readable, calling loop_read")
#             client.loop_read()

#         self.loop.add_reader(sock, cb)
#         self.misc = self.loop.create_task(self.misc_loop())

#     def on_socket_close(self, client, userdata, sock):
#         logger.debug("Socket closed")
#         self.loop.remove_reader(sock)
#         self.misc.cancel()

#     def on_socket_register_write(self, client, userdata, sock):
#         logger.debug("Watching socket for writability.")

#         def cb():
#             logger.debug("Socket is writable, calling loop_write")
#             client.loop_write()

#         self.loop.add_writer(sock, cb)

#     def on_socket_unregister_write(self, client, userdata, sock):
#         logger.debug("Stop watching socket for writability.")
#         self.loop.remove_writer(sock)

#     async def misc_loop(self):
#         logger.debug("misc_loop started")
#         while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
#             try:
#                 await asyncio.sleep(1)
#             except asyncio.CancelledError:
#                 break
#         logger.debug("misc_loop finished")


# class AsyncMQTTNode(MQTTNode):
#     def __init__(
#         self, loop, broker_config, name=None, node_id="", node_type=None, logger=None
#     ):
#         super().__init__(broker_config, name, node_id, node_type)
#         self.loop = loop
#         self.client = mqtt.Client(
#             mqtt.CallbackAPIVersion.VERSION2, client_id=self.node_id, protocol=mqtt.MQTTv5
#         )
#         self.client.username_pw_set(self._username, self._password)
#         if logger:
#             self.client.enable_logger(logger)
#         self.client.on_connect = self.on_connect
#         self.client.on_message = self.on_message
#         self.client.on_disconnect = self.on_disconnect
#         self.client.on_publish = self.on_publish
#         self.client.on_subscribe = self.on_subscribe
#         self.client.on_unsubscribe = self.on_unsubscribe
#         self.client.on_log = self.on_log

#     # Main Methods
#     # ***************************************************************************

#     def subscribe(self, topic:Union[str, tuple, list[tuple]]=("#", 0), qos:int=0):
#         '''
#         Subscribe to a topic
#         :topic: Can be a string, a tuple, or a list of tuple of format (topic, qos). Both topic and qos must
#                be present in all of the tuples.
#         :qos, options and properties: Not used.

#         e.g. subscribe("my/topic", 2)
#         subscribe("my/topic", options=SubscribeOptions(qos=2))
#         subscribe(("my/topic", 1))
#         subscribe([("my/topic", 0), ("another/topic", 2)])
#         '''
#         result = self.client.subscribe(topic, qos)
#         if result[0] == 4:
#             logger.error(f"Failed to subscribe to topic: {topic}", extra={"reason_code": mqtt.error_string(result[0])})
#         else:
#             logger.info(f"Subscribed to topic: {topic}")

#     def unsubscribe(self, topic:Union[str, list[str]]):
#         '''
#         :param topic: A single string, or list of strings that are the subscription
#             topics to unsubscribe from.
#         :param properties: (MQTT v5.0 only) a Properties instance setting the MQTT v5.0 properties
#             to be included. Optional - if not set, no properties are sent.
#         '''
#         return self.client.unsubscribe(topic)

#     def publish(self, topic, payload, qos=0, retain=False):

#         return self.client.publish(topic, payload, qos, retain)

#     async def main(self):
#         self.disconnected = self.loop.create_future()
#         self.got_message = None

#         aioh = AsyncioHelper(self.loop, self.client)
#         self.client.connect(self.hostname, self.port, self.keepalive)
#         self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
#         self.client.subscribe(topic="node_0/category", qos=2)

#         try:
#             while True:
#                 self.got_message = self.loop.create_future()
#                 msg = await self.got_message
#                 logger.info("Got response with {} bytes".format(len(msg)))
#                 self.got_message = None
#         finally:
#             self.client.disconnect()
#             logger.info("Disconnected: {}".format(await self.disconnected))

#     # Callbacks
#     # ***************************************************************************

#     def on_pre_connect(self, client, userdata):
#         logger.info(f"Connecting to broker at {client.host}:{client.port}")

#     def on_connect(self, client, userdata, flags, reason_code, properties):
#         logger.info(f"Connected to broker at {client.host}:{client.port}")
#         # client.subscribe(topic)

#     def on_connect_fail(self, client, userdata):
#         logger.error("Failed to connect")

#     def on_disconnect(self, client, userdata, reason_code, properties):
#         self.disconnected.set_result(reason_code)

#     def on_message(self, client, userdata, message):
#         if not self.got_message:
#             logger.error("Got unexpected message: {}".format(message.decode()))
#         else:
#             self.got_message.set_result(message.payload)

#     def on_publish(self, client, userdata, mid, reason_code, properties):
#         logger.debug("Published message: {}".format(mid))

#     def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
#         logger.info("Subscribed to topic")

#     def on_unsubscribe(self, client, userdata, mid, properties, reason_codes):
#         logger.info("Unsubscribed from topic")

#     def on_log(self, client, userdata, level, buf):
#         logger.debug("Log: {}".format(buf))


# # print("Starting")
# # loop = asyncio.get_event_loop()
# # loop.run_until_complete(AsyncMqttExample(loop).main())
# # loop.close()
# # print("Finished")
