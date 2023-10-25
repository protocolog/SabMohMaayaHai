import time
import random
import threading

# Simulated node identifiers (replace with actual node names or IDs)
NODES = ["Node 1", "Node 2", "Node 3"]

# Path to the file that stores the current head node information
HEAD_NODE_FILE = "head_node.txt"

# Mutex for synchronization
mutex = threading.Lock()

def get_current_head():
    # Read the current head node information from the file
    try:
        with open(HEAD_NODE_FILE, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def set_current_head(head_node):
    # Write the current head node information to the file
    with open(HEAD_NODE_FILE, "w") as file:
        file.write(head_node)

def initiate_leadership_transfer(new_head):
    global mutex

    # Acquire the mutex to ensure exclusive access
    mutex.acquire()

    try:
        # Update the head node information
        set_current_head(new_head)
        print(f"Leadership transferred to {new_head}")
    finally:
        # Release the mutex
        mutex.release()

def check_for_head():
    # Simulate a custom leader election mechanism
    # In this example, the head node is chosen randomly
    return random.choice(NODES)

def main():
    while True:
        # Check the current head node from the file
        current_head = get_current_head()

        # Simulate a custom leader election mechanism
        new_head = check_for_head()

        # Update the head node if it has changed
        if new_head != current_head:
            initiate_leadership_transfer(new_head)

        # Simulate work or tasks performed by the head node
        if current_head == "Node 1":
            # Node 1-specific tasks
            print("Node 1 is performing head node tasks.")
        elif current_head == "Node 2":
            # Node 2-specific tasks
            print("Node 2 is performing head node tasks.")
        elif current_head == "Node 3":
            # Node 3-specific tasks
            print("Node 3 is performing head node tasks.")

        # Sleep for a while before checking again (adjust as needed)
        time.sleep(10)

if __name__ == "__main__":
    main()




    
