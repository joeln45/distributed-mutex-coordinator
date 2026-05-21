package com.joelnirmal.dme;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.LoggerFactory;

/**
 * Manages token granting and retrieval for nodes.
 * Supports graceful shutdown to ensure proper cleanup of resources.
 */
public class C_mutex extends Thread {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(C_mutex.class);

    C_buffer buffer;
    Socket s;
    int port;
    String n_host;
    int n_port;
    private ServerSocket ss_back;
    private boolean shutdownRequested = false;

    public C_mutex(C_buffer b, int p) {
        buffer = b;
        port = p;
    }

    /**
     * Requests a graceful shutdown: closes the return socket and interrupts this thread.
     */
    public synchronized void requestShutdown() {
        shutdownRequested = true;
        try {
            if (ss_back != null && !ss_back.isClosed()) {
                ss_back.close();
            }
        } catch (IOException e) {
            log.warn("Exception when closing mutex socket", e);
        }
        this.interrupt();
    }

    public synchronized boolean isShutdownRequested() {
        return shutdownRequested;
    }

    @Override
    public void run() {
        try {
            int returnPort = DmeConfig.getInt("coordinator.return.port");
            ss_back = new ServerSocket(returnPort);
            log.info("Listening on port {} for token returns", returnPort);

            while (!isShutdownRequested() && !buffer.isShutdownInitiated()) {
                int bufferSize = buffer.size();
                log.debug("Queue length: {}", bufferSize);

                RequestItem request = buffer.getNext();

                if (request == null && buffer.isShutdownInitiated()) {
                    break;
                }

                if (request != null) {
                    n_host = request.getHost();
                    n_port = Integer.parseInt(request.getPort());
                    int priority = request.getPriorityLevel();
                    String priorityInfo = priorityLabel(priority);

                    log.info("Granting token to {}:{}{}", n_host, n_port, priorityInfo);

                    try (Socket tokenSocket = new Socket(n_host, n_port);
                         PrintWriter out = new PrintWriter(tokenSocket.getOutputStream(), true)) {
                        out.println("TOKEN");
                        log.info("Token granted to {}:{}{}", n_host, n_port, priorityInfo);
                    } catch (IOException e) {
                        log.error("Failed to grant token to {}:{} - {}", n_host, n_port, e.getMessage());
                    }

                    try {
                        Socket returnSocket = ss_back.accept();
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(returnSocket.getInputStream()))) {
                            String message = in.readLine();
                            log.info("Token returned from {}:{}{} with message: {}",
                                    n_host, n_port, priorityInfo, message);
                        }
                        returnSocket.close();
                    } catch (IOException e) {
                        if (!isShutdownRequested()) {
                            log.error("Failed to receive token back from {}:{} - {}",
                                    n_host, n_port, e.getMessage());
                        }
                    }
                }
            }

            log.info("Shutting down mutex handler");
            try {
                if (ss_back != null && !ss_back.isClosed()) {
                    ss_back.close();
                }
            } catch (IOException e) {
                log.warn("Exception when closing mutex socket", e);
            }
        } catch (Exception e) {
            if (!isShutdownRequested()) {
                log.error("Fatal error in mutex thread", e);
            }
        } finally {
            log.info("Mutex thread terminating");
            try {
                if (ss_back != null && !ss_back.isClosed()) {
                    ss_back.close();
                    log.debug("Closed server socket");
                }
            } catch (IOException e) {
                log.warn("Error closing mutex socket", e);
            }
        }
    }

    private static String priorityLabel(int level) {
        switch (level) {
            case 1: return " (HIGH PRIORITY)";
            case 2: return " (MEDIUM PRIORITY)";
            case 3: return " (LOW PRIORITY)";
            default: return " (UNKNOWN PRIORITY)";
        }
    }
}
