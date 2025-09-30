import time
import threading
from minimq_client import MiniMqProducer, MiniMqConsumer, MiniMqError

# --- Consumer Setup ---

# 1. Instantiate the consumer
consumer = MiniMqConsumer(host='localhost', port=5677)

# 2. Use the decorator to define a listener function
@consumer.listener(topic="USER_EVENTS")
def handle_user_events(message: dict):
    """This function will be called for every message on the USER_EVENTS topic."""
    print(f"\n[CONSUMER] Received user event: {message}")
    # Simulate some work
    time.sleep(1)
    print(f"[CONSUMER] Finished processing event for user: {message.get('username')}")

# --- Producer Logic ---

def run_producer():
    """A function to run in a separate thread to send messages."""
    producer = MiniMqProducer(host='localhost', port=5677)
    print("\n[PRODUCER] Producer thread started. Will send messages every 3 seconds.")
    time.sleep(2) # Give consumer a moment to connect

    for i in range(5):
        user_data = {
            "username": f"user_{i}",
            "event": "login",
            "timestamp": time.time()
        }
        try:
            print(f"[PRODUCER] Sending login event for {user_data['username']}...")
            producer.send("USER_EVENTS", user_data)
        except MiniMqError as ex:
            print(f"[PRODUCER] Error sending message: {ex}")
        time.sleep(3)

    print("[PRODUCER] Finished sending messages. Signaling consumer to stop.")
    # After producer is done, we can stop the consumer
    # In a real app, the consumer would run indefinitely
    consumer.stop()

# --- Main Execution ---

if __name__ == "__main__":
    # 1. Start the producer in a background thread
    producer_thread = threading.Thread(target=run_producer)
    producer_thread.start()

    # 2. Start the consumer. This will block the main thread until interrupted.
    # The producer thread will call consumer.stop() when it's done.
    try:
        consumer.start(block=True)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Ensure the producer thread is also finished before exiting
        producer_thread.join()
        print("Application finished.")