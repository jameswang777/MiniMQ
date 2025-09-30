import json
import socket
import threading
import time
from collections import defaultdict

class MiniMqError(Exception):
    """Custom exception for MiniMQ client errors."""
    pass

# --- PRODUCER IMPLEMENTATION ---

class MiniMqProducer:
    """A client for producing messages to the MiniMQ broker."""

    def __init__(self, host='localhost', port=5677):
        self.host = host
        self.port = port

    def send(self, topic: str, message: dict):
        """
        Serializes a message to JSON and sends it to the specified topic.

        Args:
            topic: The topic to send the message to.
            message: A dictionary or object that can be serialized to JSON.

        Raises:
            MiniMqError: If the connection to the broker fails.
        """
        try:
            message_content = json.dumps(message)
        except TypeError as e:
            raise MiniMqError(f"Message is not JSON serializable: {e}") from e

        # IMPORTANT: The \n at the end is crucial for Java's readLine()
        command = f"PRODUCE:{topic}:{message_content}\n"

        try:
            # For simplicity, we create a new connection for each send.
            # For high performance, a connection pool would be used here.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.host, self.port))
                sock.sendall(command.encode('utf-8'))
        except ConnectionRefusedError:
            raise MiniMqError(f"Connection refused by broker at {self.host}:{self.port}")
        except Exception as e:
            raise MiniMqError(f"An unexpected error occurred during send: {e}") from e

# --- CONSUMER IMPLEMENTATION ---

class MiniMqConsumer:
    """A client for consuming messages using a decorator-based listener pattern."""

    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
        self._listeners = defaultdict(list)
        self._threads = []
        self._running = threading.Event()

    def listener(self, topic: str):
        """Decorator to register a function as a listener for a topic."""
        def decorator(func):
            self._listeners[topic].append(func)
            return func
        return decorator

    def _listener_loop(self, topic: str, handler):
        """The main loop for a single listener thread."""
        while self._running.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.host, self.port))
                    # makefile allows for easy line-by-line reading
                    with sock.makefile('rwb') as f:
                        print(f"[Listener-{topic}] Connected and listening...")
                        while self._running.is_set():
                            f.write(f"CONSUME:{topic}\n".encode('utf-8'))
                            f.flush()

                            line = f.readline().decode('utf-8').strip()
                            if not line:
                                print(f"[Listener-{topic}] Broker disconnected. Reconnecting...")
                                break  # Break inner loop to reconnect

                            if line == "NO_MSG":
                                time.sleep(1)  # Wait before polling again
                                continue

                            self._process_message(line, handler, f)

            except Exception as e:
                print(f"[Listener-{topic}] Error: {e}. Reconnecting in 5 seconds...")
                time.sleep(5)

    def _process_message(self, raw_message, handler, file_handle):
        parts = raw_message.split(':::', 2)
        if len(parts) != 3:
            print(f"Malformed message received: {raw_message}")
            return

        message_id, _, message_content = parts
        try:
            payload = json.loads(message_content)
            # Call the user's decorated function
            handler(payload)
            # Send ACK on success
            file_handle.write(f"ACK:{message_id}\n".encode('utf-8'))
            file_handle.flush()
        except Exception as e:
            print(f"Error processing message {message_id}: {e}. Message will be re-queued after timeout.")
            # We don't send ACK, so the message will be re-delivered after timeout

    def start(self, block=True):
        """Starts the consumer, launching a thread for each registered listener."""
        if self._running.is_set():
            print("Consumer is already running.")
            return

        self._running.set()
        for topic, handlers in self._listeners.items():
            for handler in handlers:
                thread = threading.Thread(target=self._listener_loop, args=(topic, handler), daemon=True)
                self._threads.append(thread)
                thread.start()

        print(f"Started {len(self._threads)} listener threads.")

        if block:
            try:
                # Keep the main thread alive to allow background threads to run
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Keyboard interrupt received. Shutting down...")
                self.stop()

    def stop(self):
        """Stops all listener threads."""
        if self._running.is_set():
            print("Stopping consumer...")
            self._running.clear()
            for thread in self._threads:
                thread.join(timeout=2) # Wait briefly for threads to exit gracefully
            self._threads = []
            print("Consumer stopped.")