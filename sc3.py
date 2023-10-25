from confluent_kafka import Consumer, KafkaError
import base64
import io
import os
import threading,time
import socket

SERVER_IP = '172.20.10.7'
SERVER_PORT = 12338
topic = "deepak"
print("[[ Connecting with head node... ]]")
print(f"[[  Node Name:  {topic}  ]]")
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((SERVER_IP, SERVER_PORT))
client_socket.send(topic.encode("utf-8"))
print("[[ Connected with kafka server ]]")
# time.sleep(2)
conf = {
    'bootstrap.servers': '172.20.10.7:9092',  
    'group.id': 'image-consumer-group', 
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])
def send_heartbeats():
    while True:
        client_socket.send(b"Heartbeat")
        time.sleep(3)
# Create a thread for seßßßnding heartbeats
heartbeat_thread = threading.Thread(target=send_heartbeats)
heartbeat_thread.daemon = True
heartbeat_thread.start()
try:
    print("[[ Waiting for head to send data... ]]")
    while True:
        # pass
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"[[ Reached end of partition {msg.partition()} ]]")
            else:
                print(f"[[ Error while consuming message: {msg.error()} ]]")
        else:
            headers = msg.headers()
            filename_header = headers and headers[0]
            if filename_header:
                filename = filename_header[1].decode("utf-8")
                file_path = os.path.join("/Users/deepaksoni/Documents/coding/DSTNproject/rcv3/", filename)
                with open(file_path, "wb") as file:
                    file.write(base64.b64decode(msg.value()))
                print(f"[[ ;) Received and saved {filename} image to {file_path} ]]")

except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
heartbeat_thread.join()