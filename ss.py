import base64
import os
import io
import random
import sys
from datetime import datetime
from confluent_kafka import Producer,Consumer,KafkaError
# import pymongo
import threading
import time 
import socket
# conf = {
#     'bootstrap.servers': '172.20.10.6:9092',  # Replace with your Kafka broker(s)
#     'group.id': 'image-consumer',  # Replace with your consumer group ID
#     'auto.offset.reset': 'earliest'
# }
# consumer = Consumer(conf)
# topic = "coco2017"  # Replace with your topic name
# consumer.subscribe([topic])
nodes = ['atharva','deepak','tarun']

# deadmf = {}
# nodes = {'atharva':0}
# minHeap = [(data_sent,name) for name,data_sent in nodes.items()]
client_heartbeats_list = {}

files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/coco2017/")
random.shuffle(files)
MAX_LIMIT = 10
kafka_conf = {
    'bootstrap.servers': '192.168.100.84:9092'  # Replace with your Kafka broker(s)
}
producer = Producer(kafka_conf)

# Define the server IP address and port
# SERVER_IP = '192.168.100.25'
SERVER_IP = '192.168.100.84'
SERVER_PORT = 12347
client_heartbeats = {}

# Function to handle client connections
def handle_client(client_socket, client_address):
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        # Update the last heartbeat time for the client
        client_heartbeats[client_address] = time.time()
        print(f"Heartbeat received from {client_address}")
    client_socket.close()

# Function to check for dead clients
def check_dead_clients():
    while True:
        current_time = time.time()
        for client_address, last_heartbeat in list(client_heartbeats.items()):
            if current_time - last_heartbeat > 3:
                print(f"Client {client_address} is dead.")
                del client_heartbeats[client_address]
        time.sleep(1)

# Create a socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(5)

print(f"Server is listening on {SERVER_IP}:{SERVER_PORT}")

# Start a thread to check for dead clients
dead_client_checker = threading.Thread(target=check_dead_clients)
dead_client_checker.daemon = True
dead_client_checker.start()
client, addr = server_socket.accept()
mapping['']
print(f"Accepted connection from {addr}")
# client_heartbeats_list['atharva'] = addr
# Start a new thread to handle the client
client_handler = threading.Thread(target=handle_client, args=(client, addr))
client_handler.start()
try:
     while MAX_LIMIT>0:
            # size , topic = heapq.heappop(minHeap)
            # deadmf[topic] = size
            topic = random.choice(nodes)
            #topic = random.choice(allnodes)
            while(topic not in client_heartbeats):
                topic = random.choice(nodes)
            image_file = files.pop()
            with open(f"/Users/deepaksoni/Documents/coding/DSTNproject/coco2017/{image_file}", "rb") as file:
                    image_data = file.read()
            image_data_base64 = base64.b64encode(image_data).decode('utf-8')
            producer.produce(topic, value=image_data_base64)
            print("Image sent successfully to: ",topic)
            producer.flush()
            MAX_LIMIT-=1
            # while len(deadmf) != 0:
            #     heapq.heappush(minHeap,deadmf.pop())
            # repsize , reptopic = heapq.heappop(minHeap)
            # deadmf[reptopic] = repsize
            # while(reptopic == topic and reptopic not in client_heartbeats):
            #     deadmf[reptopic] = repsize
            #     repsize, reptopic = heapq.heappop(minHeap)
            # if(reptopic in client_heartbeats):
            #     image_file = files.pop()
            #     with open(f"/Users/tarunsingh/Documents/coco2017/{image_file}", "rb") as file:
            #             image_data = file.read()
            #     image_data_base64 = base64.b64encode(image_data).decode('utf-8')
            #     producer.produce(reptopic, value=image_data_base64)
            #     print("Replication sent successfully to: ",reptopic)
            #     heapq.heappush(minHeap, (repsize+len(image_data) ,reptopic))
            #     producer.flush()
            # while len(deadmf) != 0:
            #     heapq.heappush(minHeap,deadmf.pop())







#         # Metadata
# #     metadata = {
# #         "filename": image_file,
# #         "modified_date": datetime.now(),
# #         "file_size_bytes": len(image_data),
# #         "sent_to_node": topic,  # Oxriginal node
# #         "replicated_to_nodes": []  # Initialize with an empty list
# #     }

# #     # Replicate the image to multiple node topics with a replication factor of 2
# #     random.shuffle(node_topics)  # Randomly shuffle the node topics
# #     replicated_to_nodes = node_topics[:2]  # Store the replicated nodes
# #     for node_topic in replicated_to_nodes:
# #         if node_topic != topic:
# #             producer.produce(node_topic, value=image_data_base64)
# #             print(f"Replicated image to topic: {node_topic}")
# #             metadata["replicated_to_nodes"].append(node_topic)
# #     # Insert metadata into the MongoDB collection
# #     metadata_collection.insert_one(metadata)
# #     num -= 1
#         # Wait for any outstanding messages to be delivered
except KeyboardInterrupt:
        print("Exiting...")

client_handler.join()
dead_client_checker.join()