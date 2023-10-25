import base64
import os
import random
from datetime import datetime
from confluent_kafka import Producer,Consumer,KafkaError
import threading
import time 
import socket
from collections import defaultdict 

topic = 'coco2017'
client_heartbeats_list = {}
files = os.listdir("C:\\Users\\damle\\Downloads\\coco2017\\train2017")
random.shuffle(files)
limit = int(input("[[ Enter number of images want to send: "))
kafka_conf = {
    'bootstrap.servers': '192.168.100.201:9093'  # Replace with your Kafka broker(s)
}
producer = Producer(kafka_conf)
# SERVER_IP = '192.168.100.25'
SERVER_IP = '192.168.100.201'
SERVER_PORT = 12348
# Function to handle client connections
print("[[ Creating socket... ]]")
# Create a socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(5)
print(f"[[ Server is listening on {SERVER_IP}:{SERVER_PORT} ]]")
client, addr = server_socket.accept()
print(f"[[ Accepted connection from {addr} ]]")
client.send(str(limit).encode("utf-8"))
try:
    while limit>0:
            image_file = files.pop()
            file_path = f"C:\\Users\\damle\\Downloads\\coco2017\\train2017\\{image_file}"
            with open(file_path, "rb") as file:
                    image_data = file.read()
            image_data_base64 = base64.b64encode(image_data).decode('utf-8')
            producer.produce(topic, value=image_data_base64, key=os.path.basename(file_path),headers=[("filename", os.path.basename(file_path).encode("utf-8"))])
            print("[[Image sent successfully to: ",topic)
            producer.flush()
            limit-=1
except KeyboardInterrupt:
        print("Exiting...")

client_handler.join()
dead_client_checker.join()