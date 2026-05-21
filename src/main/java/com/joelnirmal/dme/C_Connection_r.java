package com.joelnirmal.dme;

import java.io.*;
import java.net.*;

/**
 * This class handles individual connections from nodes to the Coordinator.
 * It processes incoming requests, validates them, and interacts with the shared buffer.
 * This class also handles shutdown requests and sends appropriate responses back to the node.
 */
public class C_Connection_r extends Thread {
    C_buffer buffer; // Shared buffer to store requests
    Socket s; // Socket for communication with the node
    InputStream in; // Input stream for reading data from the socket
    BufferedReader bin; // Buffered reader for easier input handling

    /**
     * Constructor for the connection handler.
     * Initializes the socket and shared buffer for processing requests.
     *
     * @param s The socket for communication with the node.
     * @param b The shared buffer for storing requests.
     */
    public C_Connection_r(Socket s, C_buffer b) {
        this.s = s;
        this.buffer = b;
    }

    /**
     * The main logic for handling a connection.
     * Reads the request from the node, validates it, and processes it.
     * Handles shutdown requests and sends appropriate responses back to the node.
     */
    public void run() {
        final int NODE = 0; // Index for the node's hostname in the request array
        final int PORT = 1; // Index for the node's port number in the request array
        final int PRIORITY = 2; // Index for the priority level in the request array
        final int SHUTDOWN = 3; // Index for the shutdown flag in the request array

        String[] request = new String[4]; // Array to store the request details

        try {
            // Initialize input streams for reading data from the socket
            in = s.getInputStream();
            bin = new BufferedReader(new InputStreamReader(in));
            s.setSoTimeout(DmeConfig.getInt("coordinator.connection.read.timeout.ms"));

            // Read all parts of the request (hostname, port, priority, shutdown flag)
            for (int i = 0; i < 4; i++) {
                request[i] = bin.readLine();
                if (request[i] == null) {
                    Logger.log("Coordinator-Connection", "error: Incomplete request from " + s.getInetAddress());
                    s.close();
                    return;
                }
            }

            // Validate the port number
            try {
                Integer.parseInt(request[PORT]);
            } catch (NumberFormatException e) {
                Logger.log("Coordinator-Connection", "error: Invalid port number from " + request[NODE]);
                s.close();
                return;
            }

            // Handle shutdown requests
            if (Boolean.parseBoolean(request[SHUTDOWN])) {
                Logger.log("Coordinator-Connection", 
                        "Received SHUTDOWN request from " + request[NODE] + ":" + request[PORT]);
                buffer.initiateShutdown();

                // Send a shutdown acknowledgment to the node
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println("SHUTDOWN");
                s.close();

                // Terminate the Coordinator
                System.exit(0);
                return;
            }

            // Process the priority level from the request
            int priorityLevel = 3; // Default to low priority
            try {
                priorityLevel = Integer.parseInt(request[PRIORITY]);
                if (priorityLevel < 1 || priorityLevel > 3) {
                    priorityLevel = 3; // Default to low if invalid
                }
            } catch (NumberFormatException e) {
                priorityLevel = 3; // Default to low if parsing fails
            }

            // Save the request to the buffer (only the first three parameters)
            String[] requestData = {request[NODE], request[PORT], String.valueOf(priorityLevel)};
            buffer.saveRequest(requestData);

            // Log the priority level of the request
            String priorityInfo;
            switch (priorityLevel) {
                case 1: priorityInfo = " (HIGH PRIORITY)"; break;
                case 2: priorityInfo = " (MEDIUM PRIORITY)"; break;
                case 3: priorityInfo = " (LOW PRIORITY)"; break;
                default: priorityInfo = " (UNKNOWN PRIORITY)";
            }
            Logger.log("Coordinator-Connection", "Received request from " +
                    request[NODE] + ":" + request[PORT] + priorityInfo);

            // Send an acknowledgment back to the node
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("REQUEST_ACK");

        } catch (IOException e) {
            // Handle errors during communication
            if (!buffer.isShutdownInitiated()) {
                Logger.log("Coordinator-Connection", "ERROR: " + e.getMessage());
                System.err.println("Connection error with node: " + e.getMessage());
            }
        } finally {
            // Ensure the socket is closed and log the closure
            try {
                if (s != null) {
                    s.close();
                    Logger.log("Coordinator-Connection", "Closed connection with the node");
                }
            } catch (IOException e) {
                Logger.log("Coordinator-Connection", "Error closing socket: " + e.getMessage());
            }
        }

        // Display the current state of the buffer
        buffer.show();
    }
}