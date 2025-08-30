#!/usr/bin/env python3
import json, time
from paho.mqtt import client as mqtt_client
from paho.mqtt.subscribeoptions import SubscribeOptions
from queue import Queue
#from datetime import datetime

class MqttClientData:
    def __init__(self, topic, node_id, user_id, client, max_cue_lenght=10, last_packet=None):
        self.topic = topic
        self.node_id = node_id
        self.user_id = user_id
        self.client = client
        self.packet_queue = Queue(maxsize=max_cue_lenght)
        #self.last_packet = last_packet if last_packet is not None else {}

class PacketHistory:
    def __init__(self, history_lenght=100):
        self.cue = []
        self.history_lenght = history_lenght

    def add_new(self, packet):
        self.cue.append(packet)
        if len(self.cue) > self.history_lenght:
            self.cue.pop(0)

    def check_presence(self, packet):
        if packet in self.cue:
            return True
        else:
            self.add_new(packet)
            return False
        
    #def maintain(self, time_difference=86400):
    #    if time_difference > 0 and isinstance(time_difference, int):
    #        if len(self.cue) > 2:
    #            latest_timestamp = self.cue[-1]["timestamp"]
    #            
    #            x = 0
    #            while x < len(self.cue):
    #                if self.cue[x]["timestamp"] < latest_timestamp - time_difference:
    #                    self.cue.pop(x)
    #                else:
    #                    x = x+1
    #    else:
    #        print("Call error: time_difference must be a positive int. I didnt do nothing")
    #        return


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        options = SubscribeOptions(qos=1, noLocal=True)
        for temp in mqtt_data:
            client.subscribe(temp.topic, options=options)
    else:
        print(f"Connection failed rc{rc}")

def on_message(client, userdata, msg):
    #try:
    #    data = json.loads(msg.payload)
    #    ## Process the JSON data as needed
    #    #if data["sender"] == userdata.user_id:
    #        userdata.packet_queue.put(data)
    #except Exception as e:
    #    print(f"Failed to process JSON data due to error: {e}")
    userdata.packet_queue.put(msg)
    

def publish(client, topic, message):
    #result = client.publish(topic+"", json.dumps(message))
    result = client.publish(topic+"", message)
    result.wait_for_publish()
    if result[0] == 0:
        print(f"Successfully published")
    else:
        print("Failed to publish message")


def message_handler(mqtt_data, client_data, data):
    ##check if packet has already been received
    #if data["sender"] == client_data.user_id:
    #    if data == client_data.last_packet:
    #        print("packet already processed")
    #        return  #packet already processed last time
    #    if client_data.last_packet in dir():    #if value is set
    #        if data["timestamp"] < client_data.last_packet["timestamp"]:    #if value is old
    #            print("Old message, trashed")
    #            return #old message
    #else: 
    #    print("packet received from self")
    #    return  #trash everything pubblished different from the specified meshtastic client id for that topic
    #
    #client_data.last_packet = data
    #    
    #i_from = data["from"]   #just for debug
    #i_to = data["to"]
    #i_timestamp = data["timestamp"]
    #print(f"Publishing message from {i_from} to {i_to} whit time {i_timestamp}")
    #if it's the first time hearing it, transmit to every topic different from origin
    for temp in mqtt_data:
        if client_data.user_id != temp.user_id:
            publish(client_data.client, temp.topic, data)


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
        if "node_number" not in data or "topic" not in data or "user_id" not in data or "channel_name" not in data:
            print(f"Error: missing properties 'node_number' or 'topic' or 'user_id' or 'channel_name' for client {data}")
            continue
        # Create new client data
        #topic = data["json_topic"]
        topic = data["topic"] + "/2/e/" + data["channel_name"] + "/" + data["user_id"]
        nid = int(data["node_number"])
        uid = data["user_id"]
        client = mqtt_client.Client(mqtt_client_id + str(len(mqtt_data)))
        mqtt_data.append(MqttClientData(topic, nid, uid, client))  # Create a new MQTT client instance
        

    if len(mqtt_data) < 2:
        print("Error: not enough topics subscribed (<2)")
        exit(1)

    for temp in mqtt_data:
        client = temp.client
        # cue passtrough to clients
        client.user_data_set(temp)
        # Set callbacks for on_connect and on_message events
        client.on_connect = on_connect
        client.on_message = on_message
        # Connect to the MQTT broker with the specified parameters
        client.username_pw_set(mqtt_client_id, mqtt_password)
        client.connect(mqtt_broker, mqtt_port)
        # Start the loop for processing incoming messages and event callbacks
        client.loop_start()

    history_keeper = PacketHistory()
    while True:
        queue_emptied = False
        for client_data in mqtt_data: 
            while client_data.packet_queue.qsize() > 1:
                packet = client_data.packet_queue.get()
                if not history_keeper.check_presence(packet):
                    message_handler(mqtt_data, client_data, packet)
                    time.sleep(1) #idk how much time does it take to propagate :)
                queue_emptied = True
        if not queue_emptied:   # If I did some work, don't sleep
            print("Queue empty, waiting...")
            time.sleep(5)   #do nothing


if __name__ == "__main__":
    main()