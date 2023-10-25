import base64
import os
import io
import random
import sys
from datetime import datetime
from confluent_kafka import Producer,Consumer,KafkaError
import pymongo
import threading
import time 
import socket
mapping = {}

print("[[ Createing socket for consumers... ]]")
SERVER_IP = '172.20.10.7'
SERVER_PORT = 12338
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(5)
print(f"[[ Server is listening on {SERVER_IP}:{SERVER_PORT} ]]")

client = pymongo.MongoClient("mongodb+srv://deepak123:Dsoni9636@users.5nc4ybo.mongodb.net/?retryWrites=true&w=majority")
db = client["DSTN"]
metadata_collection = db["metadata"]

print("[[ Waiting to connect all consumer nodes... ]]")

client1, addr1 = server_socket.accept()
print(f"[[ Accepted connection from ({addr1}) ]]")
# client.send("Enter your topic: ")
temptopic1 = client1.recv(1024).decode("utf-8")
mapping[temptopic1] = addr1




client2, addr2 = server_socket.accept()
print(f"[[ Accepted connection from ({addr2}) ]]")
# client.send("Enter your topic: ")
temptopic2 = client2.recv(1024).decode("utf-8")
mapping[temptopic2] = addr2



client3, addr3 = server_socket.accept()
print(f"[[ Accepted connection from ({addr3}) ]]")
# client.send("Enter your topic: ")
temptopic3 = client3.recv(1024).decode("utf-8")
mapping[temptopic3] = addr3

print("MAPPING:::",mapping)

print("[[ Connecting with group 1 ]]")

GRUOP_IP = '172.20.10.9'
GROUP_PORT = 11242
group_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
group_client_socket.connect((GRUOP_IP, GROUP_PORT))
length = int(group_client_socket.recv(1024).decode("utf-8"))
print(f"[[ Connected... Number of images: {length}]]")



conf = {
    'bootstrap.servers': '172.20.10.9:9093',  # Replace with your Kafka broker(s)
    'group.id': 'image-consumer',  # Replace with your consumer group ID
    'auto.offset.reset': 'earliest'
    # Other consumer configuration settings (optional)
}

consumer = Consumer(conf)
topic = "coco2017"  # Replace with your topic name
consumer.subscribe([topic])


files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/coco2017/")
random.shuffle(files)
kafka_conf = {
    'bootstrap.servers': '172.20.10.7:9092'  # Replace with your Kafka broker(s)
    
}
producer = Producer(kafka_conf)
client_heartbeats = {}
# Function to handle client connections
def handle_client(client_socket, client_address, node_name):
    try:
        while True:
            data = client_socket.recv(1024).decode("utf-8")
            if not data:
                break
            # Update the last heartbeat time for the client
            client_heartbeats[node_name] = time.time()
            print(f"v.v Heartbeat received from {node_name} ({client_address}) <3")
        client_socket.close()
    except KeyboardInterrupt:
         pass

# Function to check for dead clients
def check_dead_clients():
    try:
        while True:
            current_time = time.time()
            for client_address, last_heartbeat in list(client_heartbeats.items()):
                if current_time - last_heartbeat > 5:
                    print(f" ;( Client {client_address} is dead ;( ")
                    del client_heartbeats[client_address]
            time.sleep(1)
    except KeyboardInterrupt:
         pass


clients = [client1,client2,client3]
topic_arr = ['aatharva','tarun','deepak']
i = 0
for node_name, addr in mapping.items():
    dead_client_checker = threading.Thread(target=check_dead_clients)
    dead_client_checker.daemon = True
    dead_client_checker.start()
    client_handler = threading.Thread(target=handle_client, args=(clients[i], addr, node_name))
    client_handler.start()
    i+=1
i = 0
print("[[ Waiting for Group 1 to send data... ]]")
for node_name, addr in mapping.items():
    print("[[ TOPIC SELECTED::",node_name,"]]")
    limit = length//3
    try:
        while limit>0:
            time.sleep(1)
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"<< Reached end of partition {msg.partition()} >>")
                else:
                    print(f"<< Error while consuming message: {msg.error()} >>")
            else:
                headers = msg.headers()
                filename_header = headers and headers[0]
                if filename_header:
                    filename = filename_header[1].decode("utf-8")
                    producer.produce(
                        node_name,
                        key=filename.encode("utf-8"),
                        value=msg.value(),
                        headers=[("filename", filename.encode("utf-8"))]
                    )
                    print(f"<[[ Image is sent to: {node_name} ]]>")
                    metadata = {
                    "filename": filename,
                    "modified_date": datetime.now(),
                    "file_size_bytes": len(base64.b64decode(msg.value())),
                    "sent_to_node": node_name,  # Original node
                    "replicated_to_nodes": []  # Initialize with an empty list
                    }
                    rep = topic_arr[(i+1)%3]
                    producer.produce(rep,key=filename.encode("utf-8"),value=msg.value(),
                        headers=[("filename", filename.encode("utf-8"))])
                    print("<[[ Replication is sent successfully to: ",rep,"]]>")
                    metadata['replicated_to_nodes'].append(rep)
                    metadata_collection.insert_one(metadata)
                    producer.flush()
            limit-=1
    except KeyboardInterrupt:
        pass
    i+=1

client_handler.join()
dead_client_checker.join()