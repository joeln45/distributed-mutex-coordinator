package com.joelnirmal.dme;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.slf4j.LoggerFactory;

/**
 * Handles a single connection from a node to the Coordinator.
 * Reads the request, validates it, and routes it to the shared buffer.
 * Handles shutdown requests and sends acknowledgements back to the node.
 */
public class C_Connection_r extends Thread {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(C_Connection_r.class);

    C_buffer buffer;
    Socket s;
    InputStream in;
    BufferedReader bin;

    public C_Connection_r(Socket s, C_buffer b) {
        this.s = s;
        this.buffer = b;
    }

    @Override
    public void run() {
        final int NODE = 0;
        final int PORT = 1;
        final int PRIORITY = 2;
        final int SHUTDOWN = 3;

        String[] request = new String[4];

        try {
            in = s.getInputStream();
            bin = new BufferedReader(new InputStreamReader(in));
            s.setSoTimeout(DmeConfig.getInt("coordinator.connection.read.timeout.ms"));

            for (int i = 0; i < 4; i++) {
                request[i] = bin.readLine();
                if (request[i] == null) {
                    log.warn("Incomplete request from {}", s.getInetAddress());
                    s.close();
                    return;
                }
            }

            try {
                Integer.parseInt(request[PORT]);
            } catch (NumberFormatException e) {
                log.warn("Invalid port number from {}", request[NODE]);
                s.close();
                return;
            }

            if (Boolean.parseBoolean(request[SHUTDOWN])) {
                log.info("Received SHUTDOWN request from {}:{}", request[NODE], request[PORT]);
                buffer.initiateShutdown();

                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println("SHUTDOWN");
                s.close();

                System.exit(0); // refactored in Phase 4 into cooperative shutdown
                return;
            }

            int priorityLevel = 3;
            try {
                priorityLevel = Integer.parseInt(request[PRIORITY]);
                if (priorityLevel < 1 || priorityLevel > 3) {
                    priorityLevel = 3;
                }
            } catch (NumberFormatException e) {
                priorityLevel = 3;
            }

            String[] requestData = {request[NODE], request[PORT], String.valueOf(priorityLevel)};
            buffer.saveRequest(requestData);

            log.info("Received request from {}:{}{}", request[NODE], request[PORT], priorityLabel(priorityLevel));

            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println("REQUEST_ACK");

        } catch (IOException e) {
            if (!buffer.isShutdownInitiated()) {
                log.error("Connection error with node", e);
            }
        } finally {
            try {
                if (s != null) {
                    s.close();
                    log.debug("Closed connection with the node");
                }
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
        }

        buffer.show();
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
