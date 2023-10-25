from confluent_kafka import Consumer, KafkaError
import os

# Kafka broker configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'Deepak',  # Replace with your consumer group ID
    'auto.offset.reset': 'earliest'
    # Other consumer configuration settings (optional)
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the Kafka topic from which you want to receive image data
topic = "my-topic"  # Replace with your topic name
consumer.subscribe([topic])

# Directory to save received images
output_directory = "./received_Tarun"

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

try:
    while True:
        # Poll for messages
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition {msg.partition()}")
            else:
                print(f"Error while consuming message: {msg.error()}")
        else:
            # Received a message with image data
            image_data = msg.value()

            # Generate a unique filename for the received image
            filename = os.path.join(output_directory, f"image_{msg.partition()}_{msg.offset()}.jpg")

            # Save the image data to a file
            with open(filename, "wb") as file:
                file.write(image_data)

            print(f"Received and saved image to {filename}")

except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
