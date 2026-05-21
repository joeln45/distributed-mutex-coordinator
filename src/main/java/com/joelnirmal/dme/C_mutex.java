package com.joelnirmal.dme;

import java.io.*;
import java.net.*;

/**
 * This class is responsible for managing token granting and retrieval for nodes.
 * This class also supports graceful shutdown to ensure proper cleanup of resources.
 */
public class C_mutex extends Thread {
    C_buffer buffer;
    Socket s;
    int port;
    String n_host;
    int n_port;
    private ServerSocket ss_back;
    private boolean shutdownRequested = false;

    /**
     * Constructor for the C_mutex class.
     * Initializes the mutex handler with a shared buffer and the port number.
     *
     * @param b The shared buffer for storing and managing requests.
     * @param p The port number for the mutex handler.
     */
    public C_mutex(C_buffer b, int p) {
        buffer = b;
        port = p;
    }

    /**
     * Requests a graceful shutdown of the mutex handler.
     * This method sets the shutdown flag and closes the server socket to stop waiting for tokens.
     */
    public synchronized void requestShutdown() {
        shutdownRequested = true;
        try {
            if (ss_back != null && !ss_back.isClosed()) {
                ss_back.close();
            }
        } catch (IOException e) {
            System.out.println("Exception when closing mutex socket: " + e);
        }
        // Interrupt the thread to ensure it terminates
        this.interrupt();
    }

    /**
     * Checks if a shutdown has been requested.
     *
     * @return True if a shutdown has been requested, false otherwise.
     */
    public synchronized boolean isShutdownRequested() {
        return shutdownRequested;
    }

    /**
     * The main execution method for the mutex handler thread.
     * This method listens for token return connections, processes requests from the buffer,
     * grants tokens to nodes, and waits for the token to be returned.
     */
    public void run() {
        try {
            // Create a server socket to listen for token return connections
            int returnPort = DmeConfig.getInt("coordinator.return.port");
            ss_back = new ServerSocket(returnPort);
            System.out.println("C:mutex   Listening on port " + returnPort + " for token returns");

            // Main loop to process requests and manage tokens
            while (!isShutdownRequested() && !buffer.isShutdownInitiated()) {
                int bufferSize = buffer.size();
                System.out.println("C:mutex   Buffer size is " + bufferSize);
                Logger.log("Coordinator-Mutex", "Queue length: " + bufferSize);

                // Get the next request from the buffer
                RequestItem request = buffer.getNext();

                // If the buffer is empty and shutdown is initiated, exit the loop
                if (request == null && buffer.isShutdownInitiated()) {
                    break;
                }

                if (request != null) {
                    n_host = request.getHost();
                    n_port = Integer.parseInt(request.getPort());
                    int priority = request.getPriorityLevel();

                    String priorityInfo;
                    switch (priority) {
                        case 1: priorityInfo = " (HIGH PRIORITY)"; break;
                        case 2: priorityInfo = " (MEDIUM PRIORITY)"; break;
                        case 3: priorityInfo = " (LOW PRIORITY)"; break;
                        default: priorityInfo = " (UNKNOWN PRIORITY)";
                    }
                    System.out.println("C:mutex   Granting token to " + n_host + ":" + n_port + priorityInfo);
                    Logger.log("Coordinator-Mutex", "Granting token to " + n_host + ":" + n_port + priorityInfo);

                    // Grant the token to the requesting node
                    try {
                        Socket tokenSocket = new Socket(n_host, n_port);
                        PrintWriter out = new PrintWriter(tokenSocket.getOutputStream(), true);
                        out.println("TOKEN"); // Send the token as a simple message
                        System.out.println("C:mutex   Token granted to " + n_host + ":" + n_port + priorityInfo);
                        Logger.log("Coordinator-Mutex", "Token granted to " + n_host + ":" + n_port + priorityInfo);
                        tokenSocket.close();
                    } catch (IOException e) {
                        System.out.println("CRASH Mutex connecting to the node for granting the TOKEN: " + e);
                        Logger.log("Coordinator-Mutex", "error: Failed to grant token to " + n_host + ":" + n_port + " - " + e.getMessage());
                    }

                    // Wait for the token to be returned
                    try {
                        Socket returnSocket = ss_back.accept(); // Blocking call to wait for the token
                        BufferedReader in = new BufferedReader(new InputStreamReader(returnSocket.getInputStream()));
                        String message = in.readLine(); // Optional confirmation message
                        System.out.println("C:mutex   Token returned from " + n_host + ":" + n_port + priorityInfo + " with message: " + message);
                        Logger.log("Coordinator-Mutex", "Token returned from " + n_host + ":" + n_port + priorityInfo + " with message: " + message);
                        returnSocket.close();
                    } catch (IOException e) {
                        if (!isShutdownRequested()) {
                            System.out.println("CRASH Mutex waiting for the TOKEN back: " + e);
                            Logger.log("Coordinator-Mutex", "error: Failed to receive token back from " + n_host + ":" + n_port + " - " + e.getMessage());
                        }
                    }
                }
            }

            // Perform cleanup and shutdown the mutex handler
            System.out.println("C:mutex   Shutting down mutex handler");
            Logger.log("Coordinator-Mutex", "Shutting down mutex handler");
            try {
                if (ss_back != null && !ss_back.isClosed()) {
                    ss_back.close();
                }
            } catch (IOException e) {
                System.out.println("Exception when closing mutex socket: " + e);
            }
        } catch (Exception e) {
            if (!isShutdownRequested()) {
                System.out.println("FATAL ERROR: " + e);
                Logger.log("Coordinator-Mutex", "error: " + e.getMessage());
                e.printStackTrace();
            }
        } finally {
            System.out.println("C:mutex   Shutting down the mutex ");
            Logger.log("Coordinator-Mutex", "Shutting down mutex ");
            try {
                if (ss_back != null && !ss_back.isClosed()) {
                    ss_back.close();
                    Logger.log("Coordinator-Mutex", "Closed server socket");
                }
            } catch (IOException e) {
                System.out.println("Exception when closing mutex socket: " + e);
                Logger.log("Coordinator-Mutex", "Error closing socket: " + e.getMessage());
            }
        }
    }
}