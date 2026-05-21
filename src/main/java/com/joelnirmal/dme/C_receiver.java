package com.joelnirmal.dme;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.LoggerFactory;

/**
 * Listens for incoming requests from nodes.
 * Creates a server socket to accept connections and spawns a new thread per request.
 * Supports graceful shutdown by closing the server socket.
 */
public class C_receiver extends Thread {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(C_receiver.class);

    private C_buffer buffer;
    private int port;
    private ServerSocket s_socket;
    private Socket socketFromNode;
    private C_Connection_r connect;
    private boolean shutdownRequested = false;

    public C_receiver(C_buffer b, int p) {
        buffer = b;
        port = p;
    }

    public synchronized void requestShutdown() {
        shutdownRequested = true;
        try {
            if (s_socket != null && !s_socket.isClosed()) {
                s_socket.close();
            }
        } catch (IOException e) {
            log.warn("Exception when closing receiver socket", e);
        }
        this.interrupt();
    }

    public synchronized boolean isShutdownRequested() {
        return shutdownRequested;
    }

    @Override
    public void run() {
        try {
            s_socket = new ServerSocket(port);
            log.info("Coordinator is listening on port {}", port);
        } catch (IOException e) {
            log.error("Failed to create server socket on port {}", port, e);
            return; // graceful exit replaces System.exit(1) — refactored further in Phase 4
        }

        while (!isShutdownRequested()) {
            try {
                socketFromNode = s_socket.accept();
                log.debug("Coordinator received a connection request");

                connect = new C_Connection_r(socketFromNode, buffer);
                connect.start();
            } catch (IOException e) {
                if (!isShutdownRequested()) {
                    log.warn("Exception when accepting connection", e);
                }
            }
        }

        log.info("Shutting down receiver");
        try {
            if (s_socket != null && !s_socket.isClosed()) {
                s_socket.close();
            }
        } catch (IOException e) {
            log.warn("Error closing receiver socket", e);
        }
    }
}
