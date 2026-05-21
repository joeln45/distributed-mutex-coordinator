package com.joelnirmal.dme;

import java.io.IOException;
import java.net.*;

/**
 * This class is responsible for listening for incoming requests from nodes.
 * It creates a server socket to accept connections and spawns a new thread to handle each request.
 * This class also supports graceful shutdown by closing the server socket.
 */
public class C_receiver extends Thread {

    private C_buffer buffer; // Shared buffer to store incoming requests
    private int port; // Port number the receiver listens on
    private ServerSocket s_socket; // Server socket for accepting connections
    private Socket socketFromNode; // Socket for communication with a node
    private C_Connection_r connect; // Thread to handle individual node requests
    private boolean shutdownRequested = false; // Flag to indicate if shutdown is requested

    /**
     * Constructor for the C_receiver class.
     * Sets up the receiver with a shared buffer and the port number it will listen on.
     *
     * @param b The shared buffer for storing requests.
     * @param p The port number the receiver will listen on.
     */
    public C_receiver(C_buffer b, int p) {
        buffer = b;
        port = p;
    }

    /**
     * Requests a graceful shutdown of the receiver.
     * It ensures that the receiver thread terminates cleanly.
     */
    public synchronized void requestShutdown() {
        shutdownRequested = true;
        try {
            if (s_socket != null && !s_socket.isClosed()) {
                s_socket.close();
            }
        } catch (IOException e) {
            System.out.println("Exception when closing receiver socket: " + e);
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
     * The main execution method for the receiver thread.
     * It continues running until a shutdown is requested.
     */
    public void run() {
        // Create the server socket to listen for incoming connections
        try {
            s_socket = new ServerSocket(port);
            System.out.println("C:receiver    Coordinator is listening on port " + port);
        } catch (IOException e) {
            System.out.println("Exception when creating server socket: " + e);
            System.exit(1); // Exit if the server socket cannot be created
        }

        // Main loop to accept and handle incoming connections
        while (!isShutdownRequested()) {
            try {
                // Accept a new connection from a node
                socketFromNode = s_socket.accept();
                System.out.println("C:receiver    Coordinator has received a request...");

                // Create a separate thread to handle the request
                connect = new C_Connection_r(socketFromNode, buffer);
                connect.start();
            } catch (IOException e) {
                if (!isShutdownRequested()) {
                    System.out.println("Exception when creating a connection: " + e);
                }
            }
        }

        // Perform cleanup and shutdown the receiver
        System.out.println("C:receiver    Shutting down the receiver");
        Logger.log("Coordinator-Receiver", "Shutting down receiver");

        try {
            if (s_socket != null && !s_socket.isClosed()) {
                s_socket.close();
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}