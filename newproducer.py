# import base64
# import os
# import io
# import random
# import sys
# from datetime import datetime
# from confluent_kafka import Producer,Consumer,KafkaError
# import pymongo


# # Consumer code:
# conf = {
#     'bootstrap.servers': '10.60.9.98:9092',  # Replace with your Kafka broker(s)
#     'group.id': 'image-consumer',  # Replace with your consumer group ID
#     'auto.offset.reset': 'earliest'
#     # Other consumer configuration settings (optional)
# }

# consumer = Consumer(conf)
# topic = "coco2017"  # Replace with your topic name
# consumer.subscribe([topic])

# # Producer 



# files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/recived_from_group1/")
# random.shuffle(files)
# MAX_LIMIT = len(files)
# kafka_conf = {
#     'bootstrap.servers': '10.60.9.98:9092'  # Replace with your Kafka broker(s)
# }
# # client = pymongo.MongoClient("mongodb+srv://deepak123:Dsoni9636@users.5nc4ybo.mongodb.net/?retryWrites=true&w=majority")
# # db = client["DSTN"]
# # metadata_collection = db["metadata"]
# # Create a Kafka producer instance
# producer = Producer(kafka_conf)

# node_topics = ["naina", "deepak", "atharwa"]  # Replace with your node topics




# try:
#     print("Waiting for Group 1 to send data...")
#     while True:
#         # Poll for messages

#         msg = consumer.poll(1.0)
#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 # End of partition event
#                 print(f"Reached end of partition {msg.partition()}")
#             else:
#                 print(f"Error while consuming message: {msg.error()}")
#         else:
#             image_data_base64 = msg.value()
#             head_to_nodetopic1 = random.choice(node_topics)
#             head_to_nodetopic2 = random.choice(node_topics)
#             while head_to_nodetopic2 == head_to_nodetopic1:
#                 head_to_nodetopic2 = random.choice(node_topics)
#             producer.produce(head_to_nodetopic1, value=image_data_base64)
#             producer.produce(head_to_nodetopic2, value=image_data_base64)
#             print("Image sent successfully to: ",head_to_nodetopic1)
#             print("Replica sent successfully to: ",head_to_nodetopic2)
#             producer.flush()
#         #     image_data = base64.b64decode(image_data_base64)
#         #     num_files+=1
#         #     # image_stream = io.BytesIO(image_data)
#         #     filename = os.path.join("/Users/deepaksoni/Documents/coding/DSTNproject/recived_from_group1/", f"image_{msg.partition()}_{msg.offset()}.jpg")
#         #     with open(filename, "wb") as file:
#         #         file.write(image_data)

#         #     print(f"Received and saved image to {filename}")
#         #     comming_from_group1-=1
#             # # Open and display the image (you can also save it to a file)
#             # image = Image.open(image_stream)
#             # image.show()

# except KeyboardInterrupt:
#     pass
# finally:
#     # Close the Kafka consumer
#     consumer.close()






# # files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/recived_from_group1/")
# # random.shuffle(files)
# # MAX_LIMIT = len(files)
# # kafka_conf = {
# #     'bootstrap.servers': '172.29.223.165:9093'  # Replace with your Kafka broker(s)
# # }
# # client = pymongo.MongoClient("mongodb+srv://deepak123:Dsoni9636@users.5nc4ybo.mongodb.net/?retryWrites=true&w=majority")
# # db = client["DSTN"]
# # metadata_collection = db["metadata"]
# # # Create a Kafka producer instance
# # producer = Producer(kafka_conf)

# # The topic to which you want to send the image
# # topic = sys.argv[1] # Replace with your topic name


# # # List of topics representing different nodes
# # node_topics = ["naina", "deepak", "atharwa"]  # Replace with your node topics

# # while True:
# #     topic = input(f"Topic Name -> {node_topics}: ")
# #     if topic.lower() == "exit" or topic.lower() == "close" or topic.lower() == "stop":
# #         break 
# #     num = int(input("Enter number of files wanna send: "))
    
# # try:
# #         while num_files>0:
# #             head_to_nodetopic = random.choice(node_topics)
# #             val = random.randint(1, MAX_LIMIT-1)
# # #             # Read the image file as binary data
# #             # image_file = f"image_{val}.jpg"
# #             image_file = files.pop()
# #             with open(f"/Users/deepaksoni/Documents/coding/DSTNproject/coco2017/{image_file}", "rb") as file:
# #                 image_data = file.read()
# #             # Encode the image data as base64 (optional)
# #             image_data_base64 = base64.b64encode(image_data).decode('utf-8')
#             # Produce the image data to the Kafka topic
#         #     producer.produce(head_to_nodetopic, value=image_data_base64)
#         #     print("Image sent successfully to: ",topic)
#         #     # Metadata
#             # metadata = {
#             #     "filename": image_file,
#             #     "modified_date": datetime.now(),
#             #     "file_size_bytes": len(image_data),
#             #     "sent_to_node": topic,  # Original node
#             #     "replicated_to_nodes": []  # Initialize with an empty list
#             # }

#             # Replicate the image to multiple node topics with a replication factor of 2
#             # Randomly shuffle the node topics
# #             while True:
# #                 repnode = random.choice(node_topics)
# #                 if repnode != head_to_nodetopic:
# #                         producer.produce(repnode, value=image_data_base64)
# #                         print(f"Replicated image to topic: {repnode}")
# #                         files.remove(image_file)
# #                         num_files -= 1
# #                         break
# #                         # metadata["replicated_to_nodes"].append(repnode)
# #                 # metadata_collection.insert_one(metadata)
# #             producer.flush()
# # except KeyboardInterrupt:
# #         print("Exiting...")

# # Close the Kafka producer (typically done on shutdown)
# # producer.close()



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

# conf = {
#     'bootstrap.servers': '10.60.9.98:9092',  # Replace with your Kafka broker(s)
#     'group.id': 'image-consumer',  # Replace with your consumer group ID
#     'auto.offset.reset': 'earliest'
# }

# consumer = Consumer(conf)
# topic = "coco2017"  # Replace with your topic name
# consumer.subscribe([topic])

# Producer 



# files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/data/")
# random.shuffle(files)
# MAX_LIMIT = len(files)
# kafka_conf = {
#     'bootstrap.servers': '10.60.9.98:9092'  # Replace with your Kafka broker(s)
# }

# producer = Producer(kafka_conf)

# SERVER_IP = '192.168.100.143'
SERVER_IP = '127.0.0.1'
SERVER_PORT = 12345

# Dictionary to store the last heartbeat time for each client
client_heartbeats = {}
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(1)  # Listen for a single client

print(f"Server is listening on {SERVER_IP}:{SERVER_PORT}")

# Function to handle the client connection
def handle_client(client_socket, client_address):
    while True:
        print("IN HANDLE")
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            print(data.decode("utf-8"))
            # Update the last heartbeat time for the client
            client_heartbeats[client_address] = time.time()
            print(f"Heartbeat received from {client_address}")
        except ConnectionResetError:
            print(f"Connection with {client_address} closed.")
            break
    client_socket.close()

# Accept a single client connection
client, addr = server_socket.accept()
print(f"Accepted connection from {addr}")

# Start a thread to handle the client
client_handler = threading.Thread(target=handle_client, args=(client, addr))
client_handler.start()
print("MAIN AAYA")
# Consumer code:


# node_topics = ["naina", "deepak", "atharwa"]  # Replace with your node topics
# topic = "popo2017"
# try:
#      while MAX_LIMIT>0:
#         #     head_to_nodetopic = random.choice(node_topics)
#         #     val = random.randint(1, MAX_LIMIT-1)
#         #             # Read the image file as binary data
#                 # image_file = f"image_{val}.jpg"
#                 image_file = files.pop()

#                 with open(f"/Users/deepaksoni/Documents/coding/DSTNproject/data/{image_file}", "rb") as file:
#                         image_data = file.read()
#                 # Encode the image data as base64 (optional)
#                 image_data_base64 = base64.b64encode(image_data).decode('utf-8')
#                 # Produce the image data to the Kafka topic
#                 producer.produce(topic, value=image_data_base64)
#                 print("Image sent successfully to: ",topic)
#                 producer.flush()
#                 MAX_LIMIT-=1
#         # Metadata
# #     metadata = {
# #         "filename": image_file,
# #         "modified_date": datetime.now(),
# #         "file_size_bytes": len(image_data),
# #         "sent_to_node": topic,  # Original node
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
# except KeyboardInterrupt:
#         print("Exiting...")



# # try:
# #     print("Waiting for Group 1 to send data...")
# #     while True:
# #         # Poll for messages

# #         msg = consumer.poll(1.0)
# #         if msg is None:
# #             continue
# #         if msg.error():
# #             if msg.error().code() == KafkaError._PARTITION_EOF:
# #                 # End of partition event
# #                 print(f"Reached end of partition {msg.partition()}")
# #             else:
# #                 print(f"Error while consuming message: {msg.error()}")
# #         else:
# #             image_data_base64 = msg.value()
# #             head_to_nodetopic1 = random.choice(node_topics)
# #             head_to_nodetopic2 = random.choice(node_topics)
# #             while head_to_nodetopic2 == head_to_nodetopic1:
# #                 head_to_nodetopic2 = random.choice(node_topics)
# #             producer.produce(head_to_nodetopic1, value=image_data_base64)
# #             producer.produce(head_to_nodetopic2, value=image_data_base64)
# #             print("Image sent successfully to: ",head_to_nodetopic1)
# #             print("Replica sent successfully to: ",head_to_nodetopic2)
# #             producer.flush()

# # except KeyboardInterrupt:
# #     pass
# # finally:
# #     # Close the Kafka consumer
# #     consumer.close()
client_handler.join()