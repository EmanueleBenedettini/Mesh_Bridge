#!/usr/bin/env python3
import json, time
from paho.mqtt import client as mqtt_client


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global conn
        conn = client
        for topic in mqtt_topics:
            client.subscribe(topic)
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
    print(f"Handling message from {data["sender"]...}")
    if data["sender"] is not "!849b805c":
        publish(client, mqtt_topics[1], data)
    else:
        publish(client, mqtt_topics[0], data)
    print("done handling")

def main():
    #Read configs
    with open("config.json") as f:
        config = json.load(f)
    mqtt_broker = config["broker"]["host"]
    mqtt_port = config["broker"]["port"]
    mqtt_client_id = config["broker"]["client_id"]
    mqtt_password = config["broker"]["password"]

    global mqtt_topics
    global mqtt_nodes_id
    global mqtt_user_id
    mqtt_topics = []
    mqtt_user_id = []
    mqtt_nodes_id = []

    for data in config["clients"]:
        print(data)
        if "node_number" not in data or "json_topic" not in data or "user_id" not in data:
            print(f"Error: missing properties 'node_number' or 'json_topic' or 'user_id' for client {data}")
            continue
        mqtt_nodes_id.append(int(data["node_number"]))
        mqtt_user_id.append(data["user_id"])
        mqtt_topics.append(data["json_topic"])

    if not mqtt_topics:
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

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()