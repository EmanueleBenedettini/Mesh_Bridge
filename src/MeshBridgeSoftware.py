#!/usr/bin/env python3
import json, time
from paho.mqtt import client as mqtt_client
from paho.mqtt.subscribeoptions import SubscribeOptions
from queue import Queue

class MqttClientData:
    def __init__(self, topic, node_id, user_id, client, max_cue_lenght=10, last_packet=None):
        self.topic = topic
        self.node_id = node_id
        self.user_id = user_id
        self.client = client
        packet_queue = Queue(maxsize=max_cue_lenght)
        self.last_packet = last_packet if last_packet is not None else {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global conn
        conn = client
        options = SubscribeOptions(qos=1, noLocal=True)
        for temp in mqtt_data:
            client.subscribe(temp.topic, options=options)
    else:
        print(f"Connection failed rc{rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        # Process the JSON data as needed
        #handler(client, data)
        userdata.put(data)
    except Exception as e:
        print(f"Failed to process JSON data due to error: {e}")
    

def publish(client, topic, message):
    result = client.publish(topic, json.dumps(message))
    result.wait_for_publish()
    if result[0] == 0:
        print(f"Successfully published")
    else:
        print("Failed to publish message")

##just for testing
#def handler(client, data):
##    print(f"Handling message...")
#    packet_queue.put(data)
##    print("done handling")

def message_handler(client, data):
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

    global mqtt_data    #contains a list of MqttClientData structures, one for each client
    mqtt_data = []

    for data in config["clients"]:
        print(data)
        if "node_number" not in data or "json_topic" not in data or "user_id" not in data:
            print(f"Error: missing properties 'node_number' or 'json_topic' or 'user_id' for client {data}")
            continue
        # Create new client data
        topic = data["json_topic"]
        nid = int(data["node_number"])
        uid = data["user_id"]
        client = mqtt_client.Client(mqtt_client_id + len(mqtt_data))
        mqtt_data.append(topic, nid, uid, client)  # Create a new MQTT client instance
        

    if len(mqtt_data) < 2:
        print("Error: not enough topics subscribed (<2)")
        exit(1)

    for temp in mqtt_data:
        client = temp.client
        # cue passtrough to clients
        client.user_data_set(temp.packet_queue)
        # Set callbacks for on_connect and on_message events
        client.on_connect = on_connect
        client.on_message = on_message
        # Connect to the MQTT broker with the specified parameters
        client.username_pw_set(mqtt_client_id, mqtt_password)
        client.connect(mqtt_broker, mqtt_port)
        # Start the loop for processing incoming messages and event callbacks
        client.loop_start()

    while True:
        queue_emptied = False
        for client_data in mqtt_data: 
            while client_data.packet_queue.qsize() > 1:
                message_handler(client_data.client, client_data.packet_queue.get())
                queue_emptied = True
        if not queue_emptied:   # If I did some work, don't sleep
            print("Queue empty, waiting...")
            time.sleep(2)   #do nothing


if __name__ == "__main__":
    main()