package io.github.jameswang777.minimq;

import io.github.jameswang777.minimq.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

/**
 * Handles all communication with a single connected client in a dedicated thread.
 * It parses commands, interacts with the Broker, and sends responses.
 */
@Slf4j
public class ClientHandler implements Runnable {

    // Protocol command constants
    private static final String PRODUCE_COMMAND = "PRODUCE";
    private static final String CONSUME_COMMAND = "CONSUME";
    private static final String ACK_COMMAND = "ACK";
    private static final String NO_MSG_RESPONSE = "NO_MSG";

    private final Socket clientSocket;
    private final BrokerServer broker;
    private final String clientAddress;

    public ClientHandler(Socket socket, BrokerServer broker) {
        this.clientSocket = socket;
        this.broker = broker;
        this.clientAddress = socket.getRemoteSocketAddress().toString();
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                log.trace("Received raw command from [{}]: {}", clientAddress, inputLine);

                String[] parts = inputLine.split(":", 3);
                if (parts.length < 2) {
                    log.warn("Received malformed command from [{}]: {}", clientAddress, inputLine);
                    continue;
                }

                String command = parts[0];

                switch (command) {
                    case PRODUCE_COMMAND:
                        if (parts.length == 3) {
                            Message message = new Message(parts[1], parts[2]);
                            broker.produce(message);
                            out.println(message.getId());
                        } else {
                            log.warn("Malformed PRODUCE command from [{}]: {}", clientAddress, inputLine);
                        }
                        break;

                    case CONSUME_COMMAND:
                        String topic = parts[1];
                        Message message = broker.consume(topic);
                        if (message != null) {
                            out.println(message);
                        } else {
                            out.println(NO_MSG_RESPONSE);
                        }
                        break;

                    case ACK_COMMAND:
                        String messageId = parts[1];
                        broker.acknowledge(messageId);
                        break;

                    default:
                        log.warn("Received unknown command '{}' from [{}]", command, clientAddress);
                }
            }
        } catch (SocketException e) {
            // This is a common exception when a client abruptly disconnects. Log as INFO.
            log.info("Client [{}] disconnected abruptly: {}", clientAddress, e.getMessage());
        } catch (IOException e) {
            log.error("An I/O error occurred while handling client [{}]:", clientAddress, e);
        } catch (InterruptedException e) {
            log.warn("Client handler thread for [{}] was interrupted.", clientAddress);
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                log.error("Error while closing socket for client [{}]:", clientAddress, e);
            }
            log.info("Connection with client [{}] closed.", clientAddress);
        }
    }
}