#!/usr/bin/env python3
import json, time
from paho.mqtt import client as mqtt_client
from queue import Queue

class MqttClientData:
    def __init__(self, topic, node_id, user_id, last_packet=None):
        self.topic = topic
        self.node_id = node_id
        self.user_id = user_id
        self.last_packet = last_packet if last_packet is not None else {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global conn
        conn = client
        for temp in mqtt_data:
            client.subscribe(temp.topic)
    else:
        print(f"Connection failed rc{rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        # Process the JSON data as needed
        print(userdata)
        print(data)
        handler(client, data)
    except Exception as e:
        print(f"Failed to process JSON data due to error: {e}")
    

def publish(client, topic, message):
    result = client.publish(topic, json.dumps(message))
    result.wait_for_publish()
    if result[0] == 0:
        print("Successfully published")
    else:
        print("Failed to publish message")

#just for testing
def handler(client, data):
#    print(f"Handling message...")
    packet_queue.append(data)
#    print("done handling")

def message_decoder_and_sender(client, data):
    #check if packet has already been received
    for temp in mqtt_data:
        if data == temp.last_packet:
            print("packet already processed")
            return
        else:
            if data["sender"] == temp.user_id:
                temp.last_packet = data

    #if it's the first time hearing it, transmit to every topic different from origin
    for temp in mqtt_data:
        if data["sender"] != temp.user_id:
            publish(client, temp.topic, data)

def main():
    #Read configs
    with open("config.json") as f:
        config = json.load(f)
    mqtt_broker = config["broker"]["host"]
    mqtt_port = config["broker"]["port"]
    mqtt_client_id = config["broker"]["client_id"]
    mqtt_password = config["broker"]["password"]

    global mqtt_data
    mqtt_data = []

    for data in config["clients"]:
        print(data)
        if "node_number" not in data or "json_topic" not in data or "user_id" not in data:
            print(f"Error: missing properties 'node_number' or 'json_topic' or 'user_id' for client {data}")
            continue
        mqtt_data.append(MqttClientData(data["json_topic"], int(data["node_number"]), data["user_id"]))

    if len(mqtt_data) < 2:
        print("Error: not enough topics subscribed (<2)")
        exit(1)

    # Create a new MQTT client instance
    client = mqtt_client.Client(mqtt_client_id)

    # Set callbacks for on_connect and on_message events
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the MQTT broker with the specified parameters
    client.username_pw_set(mqtt_client_id, mqtt_password)
    client.connect(mqtt_broker, mqtt_port)

    # Start the loop for processing incoming messages and event callbacks
    client.loop_start()

    queue = Queue(maxsize=50)
    while True:
        while queue.qsize() > 1:
            message_decoder_and_sender(client, queue.get())
        else:
            time.sleep(1)   #do nothing


if __name__ == "__main__":
    main()