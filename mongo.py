import pymongo
from datetime import datetime
import random as rrr
import os
from confluent_kafka import Producer

kafka_config = {
    'bootstrap.servers': 'Vlua',
    'client.id': 'sso'
}

def store_metadata(chunk_id, metadata_info):
    try:
        metadata_collection.insert_one({"_id": chunk_id, "metadata": metadata_info})
        print(f"Metadata Uploaded... Time: {metadata_info['Timestemp']}")
    except:
        print("CAN'T Upload the metadata...")


def get_metadata(chunk_id):
    metadata_doc = metadata_collection.find_one({"_id": chunk_id})
    if metadata_doc:
        return metadata_doc["metadata"]
    else:
        return None
    

client = pymongo.MongoClient("mongodb+srv://deepak123:Dsoni9636@users.5nc4ybo.mongodb.net/?retryWrites=true&w=majority")
db = client["DSTN"]
metadata_collection = db["metadata"]
val = datetime.now()
store_metadata(rrr.randint(1000000,9999999),{"Timestemp":val,"Name":"ssss","Age":23})

# Function to split the image data into chunks
def split_image_into_chunks(image_path, chunk_size):
    try:
        with open(image_path, "rb") as image_file:
            chunk_number = 0
            while True:
                chunk = image_file.read(chunk_size)
                if not chunk:
                    break
                yield chunk_number, chunk
                chunk_number += 1
    except Exception as e:
        print(f"Error splitting the image: {str(e)}")
        
def send_byte_chunks_to_kafka(byte_chunks, kafka_topic):
    try:
        for chunk_number, chunk in byte_chunks:
            producer.produce(kafka_topic, value=chunk)
            print(f"Sent chunk {chunk_number} to Kafka topic {kafka_topic}")
        producer.flush()
    except Exception as e:
        print(f"Error sending byte chunks to Kafka: {str(e)}")

# Function to store byte chunks as files in the file system
def store_byte_chunks(byte_chunks, output_directory):
    try:
        os.makedirs(output_directory, exist_ok=True)
        for chunk_number, chunk in byte_chunks:
            output_file = os.path.join(output_directory, f"chunk_{chunk_number}.bin")
            with open(output_file, "wb") as file:
                file.write(chunk)
                print(f"Stored chunk {chunk_number} as {output_file}")
    except Exception as e:
        print(f"Error storing byte chunks: {str(e)}")

# Main program
if __name__ == "__main__":
    # Define the image file path and chunk size
    image_path = "test.jpg"
    chunk_size = 1024  # Adjust the chunk size as needed Bits

    # Define the output directory where chunks will be stored
    output_directory = "image_chunks"

    # Split the image into chunks
    byte_chunks = split_image_into_chunks(image_path, chunk_size)

    # Store the byte chunks as files in the file system
    store_byte_chunks(byte_chunks, output_directory)
