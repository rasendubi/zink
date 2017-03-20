#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time, threading
import random
import json

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("zink/dce/test01/json/status")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

def on_subscribe(client, userdata, mid, granted_qos):
    print(granted_qos)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe

client.connect("127.0.0.1")

id = random.randint(0, 1000)
temp = 25
bat = 50
def send_data():
    global temp
    global bat
    client.publish("zink/dce/test01/json", json.dumps([{"id": id, "temp": temp, "bat": bat }]))
    temp += random.randint(-1, 1)
    bat += random.randint(-1, 1)
    threading.Timer(5, send_data).start()

send_data()

client.loop_forever()
