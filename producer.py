import base64
import os
import time
import sys
from datetime import datetime
from PIL import Image
from io import BytesIO
from confluent_kafka import Producer, KafkaError
import pymongo
import base64
import random
files = os.listdir("/Users/deepaksoni/Documents/coding/DSTNproject/data/")
random.shuffle(files)
MAX_LIMIT = len(files)
# Kafka broker configuration
conf = {
    'bootstrap.servers': '10.60.9.156:9092'
}
client = pymongo.MongoClient("mongodb+srv://deepak123:Dsoni9636@users.5nc4ybo.mongodb.net/?retryWrites=true&w=majority")
db = client["DSTN"]
metadata_collection = db["metadata"]
# Create a Kafka producer instance
producer = Producer(conf)
topic = sys.argv[1] 
print("Max images available: ",MAX_LIMIT)
num_images = int(input("Enter How many images you want:: "))

while num_images:
    val = random.randint(1,MAX_LIMIT-1)
    # Read the image file as binary data
    # image_file = "image_"+str(val)+".jpg"
    image_file = files.pop()
    with open(f"/Users/deepaksoni/Documents/coding/DSTNproject/data/{image_file}", "rb") as file:
        image_data = file.read()
    # Encode the image data as base64 (optional)
    image_data_base64 = base64.b64encode(image_data).decode('utf-8')
    # Produce the image data to the Kafka topic
    producer.produce(topic, value=image_data_base64)


    metadata = {
        "filename": image_file,
        "modified_date": datetime.now(),
        "file_size_bytes": len(image_data),
        "sent_to_node": topic  # Replace with the actual node name
    }
    metadata_collection.insert_one(metadata)


    num_images -= 1
# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
