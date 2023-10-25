from confluent_kafka import Consumer, KafkaError
import base64
import io
import os
conf = {
    'bootstrap.servers': '10.60.9.151:9092',  # Replace with your Kafka broker(s)
    'group.id': 'image-consumer-group',  # Replace with your consumer group ID
    'auto.offset.reset': 'earliest'
    # Other consumer configuration settings (optional)
}

consumer = Consumer(conf)

# Subscribe to the Kafka topic from which you want to receive the image
topic = "coco2017"  # Replace with your topic name
consumer.subscribe([topic])

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
            image_data_base64 = msg.value()
            image_data = base64.b64decode(image_data_base64)
            # image_stream = io.BytesIO(image_data)
            filename = os.path.join("/Users/deepaksoni/Documents/coding/DSTNproject/recived/", f"image_{msg.partition()}_{msg.offset()}.jpg")
            with open(filename, "wb") as file:
                file.write(image_data)

            print(f"Received and saved image to {filename}")
            # # Open and display the image (you can also save it to a file)
            # image = Image.open(image_stream)
            # image.show()

except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
