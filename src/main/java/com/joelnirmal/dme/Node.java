package com.joelnirmal.dme;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Thisclass represents a participant in the distributed mutual exclusion (DME) system.
 * Each node can request a token from the Coordinator, enter a critical section, and return the token.
 * Nodes can also request a system shutdown and perform health checks on the Coordinator.
 */
public class Node {

    private Random ra;  
    private Socket s; // Socket for communication with the Coordinator
    private PrintWriter pout = null; // Output stream for sending data to the Coordinator
    private ServerSocket n_ss;  
    private Socket n_token;  
    String c_host = "127.0.0.1";  
    int c_request_port = 7000; // Port for sending token requests to the Coordinator
    int c_return_port = 7001; // Port for returning the token to the Coordinator
    String n_host = "127.0.0.1";  
    String n_host_name; 
    int n_port; 
    boolean isPriority;  
    boolean shutdownRequested;  
    private int priorityLevel; 

    // Coordinator's health check parameters
    private static final int MAX_RETRIES = 3; // Maximum retries for communication
    private static final int RETRY_DELAY = 5000; // Delay between retries (in milliseconds)
    private static final int MAX_SHUTDOWN_RETRIES = 3;  
    private boolean inCriticalSection = false;  
    private Timer coordinatorCheckTimer;  
    private int shutdownRetryCount = 0;  
    private static final int MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS = 3;
    private int coordinatorUnavailableCount = 0;

    /**
     * Constructor for the Node class.
     * sets up the node with its hostname, port, priority status, and shutdown request status.
     *
     * @param nam The hostname of the node.
     * @param por The port number of the node.
     * @param sec The maximum delay (in milliseconds) before requesting the token.
     * @param priorityLevel The priority level of the node (1=high, 2=medium, 3=low).
     * @param shutdownRequested Whether the node will request a system shutdown.
     */
    public Node(String nam, int por, int sec, int priorityLevel, boolean shutdownRequested) {
        ra = new Random();
        n_host_name = nam;
        n_port = por;
        this.priorityLevel = priorityLevel;
        this.shutdownRequested = shutdownRequested;

        String priorityInfo = getPriorityInfo();
        System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " of dme is active ....");
        Logger.log("Node-" + n_host_name + ":" + n_port, "Node is active" + priorityInfo);

        // Start periodic health checks for the Coordinator
        startCoordinatorHealthCheck();

        // Main loop for the node's operation
        while (true) {
            try {
                // Simulate a delay before requesting the token
                int sleepTime = ra.nextInt(sec * 2);
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " sleeping for " + sleepTime + "ms before requesting token");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Sleeping for " + sleepTime + "ms before requesting token");
                Thread.sleep(sleepTime);

                // Handles the shutdown retries
                if (shutdownRequested && shutdownRetryCount >= MAX_SHUTDOWN_RETRIES) {
                    System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo +
                            " stopping after " + MAX_SHUTDOWN_RETRIES + " shutdown retries");
                    Logger.log("Node-" + n_host_name + ":" + n_port,
                            "Stopping after " + MAX_SHUTDOWN_RETRIES + " shutdown retries");
                    System.exit(0);
                }

                // Request the token from the Coordinator
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " requesting token from coordinator");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Requesting token from coordinator" + priorityInfo);

                if (!sendTokenRequest(shutdownRequested)) {
                    continue; // Retry if the request fails
                }

                // Reset shutdown request after a successful request
                if (shutdownRequested) {
                    shutdownRequested = false;
                }

                // Waits for the token from the Coordinator
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " waiting for token");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Waiting for token");

                try {
                    n_ss = new ServerSocket(n_port);
                    n_ss.setSoTimeout(30000); // Timeout for waiting for the token

                    try {
                        n_token = n_ss.accept(); // Accepts  the token
                        BufferedReader in = new BufferedReader(new InputStreamReader(n_token.getInputStream()));
                        String tokenMsg = in.readLine();

                        System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " received token: " + tokenMsg);
                        Logger.log("Node-" + n_host_name + ":" + n_port, "Received token: " + tokenMsg);
                        n_token.close();
                    } catch (SocketTimeoutException e) {
                        System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " timeout waiting for token. Coordinator could be down.");
                        Logger.log("Node-" + n_host_name + ":" + n_port, "Timeout waiting for token. Coordinator could be down.");
                        n_ss.close();
                        continue;
                    }

                    n_ss.close();
                } catch (IOException e) {
                    System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " error waiting for token: " + e);
                    Logger.log("Node-" + n_host_name + ":" + n_port, "Error waiting for token: " + e.getMessage());
                    continue;
                }

                // Enters into the critical section
                inCriticalSection = true;
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " Entering critical section");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Entering critical section" + priorityInfo);
                int criticalSectionTime = 3000 + ra.nextInt(2000); // Simulate time spent in the critical section
                Thread.sleep(criticalSectionTime);
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " Exiting critical section after " + criticalSectionTime + "ms");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Exiting critical section after " + criticalSectionTime + "ms");
                inCriticalSection = false;

                // Return the token to the Coordinator
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " returning token to coordinator");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Returning token to coordinator");
                if (!returnToken()) {
                    System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " failed to return token to coordinator after multiple attempts");
                    Logger.log("Node-" + n_host_name + ":" + n_port, "Failed to return token to coordinator after a few attempts");
                }

            } catch (InterruptedException e) {
                System.out.println(e);
                Logger.log("Node-" + n_host_name + ":" + n_port, "error: " + e.getMessage());
                System.exit(1);
            }
        }
    }

    /**
     * Sends a token request to the Coordinator.
     * This method attempts to establish a connection with the Coordinator and send the node's details.
     * If the request includes a shutdown flag, it increments the shutdown retry count.
     *
     * @param withShutdown Whether the request includes a shutdown flag.
     * @return True if the request was successfully sent, false otherwise.
     */
    private boolean sendTokenRequest(boolean withShutdown) {
        String priorityInfo = getPriorityInfo();
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                s = new Socket(c_host, c_request_port);
                pout = new PrintWriter(s.getOutputStream(), true);
                
                // Send the request components in order:
                // 1. Hostname
                pout.println(n_host_name);
                // 2. Port
                pout.println(n_port);
                // 3. Priority level (1-3)
                pout.println(priorityLevel);
                // 4. Shutdown flag
                pout.println(withShutdown);

                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo +
                        " sending request: NODE=" + n_host_name +
                        ", PORT=" + n_port +
                        ", PRIORITY=" + priorityLevel +
                        ", SHUTDOWN=" + withShutdown);
                
                s.close();
                if (withShutdown) {
                    shutdownRetryCount++;
                    if (shutdownRetryCount >= MAX_SHUTDOWN_RETRIES) {
                        System.out.println("Node " + n_host_name + ":" + n_port + 
                                         " initiating system shutdown");
                        Logger.log("Node-" + n_host_name + ":" + n_port, 
                                 "Initiating full system shutdown");
                        System.exit(0); // Terminate the entire JVM
                    }
                }
                return true;
            } catch (IOException e) {
                coordinatorUnavailableCount++;
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo +
                        " failed to connect with the coordinator (" + coordinatorUnavailableCount + 
                        "/" + MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS + "): " + e);
                
                if (coordinatorUnavailableCount >= MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS) {
                    System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + 
                            " terminating due to coordinator failure");
                    Logger.log("Node-" + n_host_name + ":" + n_port, 
                            "Terminating due to coordinator failure");
                    System.exit(0);
                }
                
                if (attempt < MAX_RETRIES - 1) {
                    try {
                        Thread.sleep(RETRY_DELAY);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns the token to the Coordinator.
     * This method attempts to establish a connection with the Coordinator and send a message indicating
     * that the token is being returned.
     *
     * @return True if the token was successfully returned, false otherwise.
     */
    private boolean returnToken() {
        String priorityInfo = isPriority ? " (PRIORITY)" : "";
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                Socket returnSocket = new Socket(c_host, c_return_port);
                PrintWriter returnOut = new PrintWriter(returnSocket.getOutputStream(), true);
                returnOut.println("TOKEN RETURNED FROM " + n_host_name + ":" + n_port + priorityInfo);
                returnSocket.close();
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + " token returned successfully");
                Logger.log("Node-" + n_host_name + ":" + n_port, "Token returned successfully");
                return true;
            } catch (IOException e) {
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo +
                        " failed to return token (attempt " + (attempt + 1) + "/" + MAX_RETRIES + "): " + e);
                Logger.log("Node-" + n_host_name + ":" + n_port,
                        "Failed to return token (attempt " + (attempt + 1) + "/" + MAX_RETRIES + "): " + e.getMessage());

                if (attempt < MAX_RETRIES - 1) {
                    try {
                        Thread.sleep(RETRY_DELAY);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return false;
    }

    /**
     * Starts a periodic health check for the Coordinator.
     * This method schedules a timer task that periodically checks if the Coordinator is responsive.
     */
    private void startCoordinatorHealthCheck() {
        coordinatorCheckTimer = new Timer("CoordinatorChecker");
        coordinatorCheckTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                checkCoordinatorHealth();
            }
        }, 10000, 30000);
    }

    /**
     * Checks the health of the Coordinator.
     * This method attempts to establish a connection with the Coordinator to verify its responsiveness.
     * If the Coordinator is unresponsive, it logs the issue.
     */
    private void checkCoordinatorHealth() {
        if (inCriticalSection || shutdownRequested) {
            return;
        }

        String priorityInfo = getPriorityInfo();
        try {
            Socket healthCheckSocket = new Socket();
            healthCheckSocket.connect(new InetSocketAddress(c_host, c_request_port), 2000);
            healthCheckSocket.close();
            coordinatorUnavailableCount = 0; // Reset counter if successful
        } catch (IOException e) {
            coordinatorUnavailableCount++;
            System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo +
                    " detected that the coordinator is not responding (" + coordinatorUnavailableCount + 
                    "/" + MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS + ")");
            Logger.log("Node-" + n_host_name + ":" + n_port,
                    "Detected that the coordinator is not responding: " + e.getMessage() + 
                    " (" + coordinatorUnavailableCount + "/" + MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS + ")");
            
            if (coordinatorUnavailableCount >= MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS) {
                System.out.println("Node " + n_host_name + ":" + n_port + priorityInfo + 
                        " terminating due to coordinator failure");
                Logger.log("Node-" + n_host_name + ":" + n_port, 
                        "Terminating due to coordinator failure");
                System.exit(0);
            }
        }
    }

    /**
     * The main method for the Node class.
     * This method initializes a Node instance with the provided command-line arguments and starts its operation.
     *
     * @param args Command-line arguments:
     *             [0] - Port number for the node.
     *             [1] - Maximum delay (in milliseconds) before requesting the token.
     *             [2] - (Optional) Priority level (1=high, 2=medium, 3=low).
     *             [3] - (Optional) Whether the node will request a system shutdown (true/false).
     */
    public static void main(String args[]) {
        String n_host_name = "";
        int n_port;
        boolean isPriority = false;
        boolean shutdownRequested = false;

        int priorityLevel = 3; // Default to low priority
        if (args.length >= 3) {
            priorityLevel = Integer.parseInt(args[2]);
            if (priorityLevel < 1 || priorityLevel > 3) {
                System.out.println("Priority must be 1 (high), 2 (medium), or 3 (low)");
                System.exit(1);
            }
            System.out.println("Node is priority level: " + priorityLevel);
        }

        try {
            InetAddress n_inet_address = InetAddress.getLocalHost();
            n_host_name = n_inet_address.getHostName();
            System.out.println("node hostname is " + n_host_name + ":" + n_inet_address);
        } catch (java.net.UnknownHostException e) {
            System.out.println(e);
            System.exit(1);
        }

        n_port = Integer.parseInt(args[0]);
        System.out.println("node port is " + n_port);

        if (args.length >= 3) {
            isPriority = Boolean.parseBoolean(args[2]);
            System.out.println("Node is " + (isPriority ? "PRIORITY" : "normal") + " node");
        }

        if (args.length == 4) {
            shutdownRequested = Boolean.parseBoolean(args[3]);
            System.out.println("Node will request system shutdown after first token cycle");
        }

        Node n = new Node(n_host_name, n_port, Integer.parseInt(args[1]), priorityLevel, shutdownRequested);
    }

    /**
     * Helper method to get the priority level as a human-readable string.
     *
     * @return A string representing the priority level.
     */
    private String getPriorityInfo() {
        switch (priorityLevel) {
            case 1: return " (HIGH PRIORITY)";
            case 2: return " (MEDIUM PRIORITY)";
            case 3: return " (LOW PRIORITY)";
            default: return " (UNKNOWN PRIORITY)";
        }
    }
}