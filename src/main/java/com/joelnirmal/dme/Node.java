package com.joelnirmal.dme;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.LoggerFactory;

/**
 * A participant in the distributed mutual exclusion (DME) system.
 *
 * <p>Each node can request a token from the Coordinator, enter a critical section,
 * and return the token. Nodes can also request a system shutdown and perform periodic
 * health checks against the Coordinator.</p>
 */
public class Node {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Node.class);

    private Random ra;
    private Socket s;
    private PrintWriter pout = null;
    private ServerSocket n_ss;
    private Socket n_token;

    private final String c_host = DmeConfig.getString("coordinator.host");
    private final int c_request_port = DmeConfig.getInt("coordinator.request.port");
    private final int c_return_port = DmeConfig.getInt("coordinator.return.port");

    String n_host_name;
    int n_port;
    boolean shutdownRequested;
    private int priorityLevel;

    // Coordinator retry / health-check parameters (loaded from application.properties)
    private static final int MAX_RETRIES = DmeConfig.getInt("node.coordinator.max.retries");
    private static final int RETRY_DELAY = DmeConfig.getInt("node.coordinator.retry.delay.ms");
    private static final int MAX_SHUTDOWN_RETRIES = DmeConfig.getInt("node.coordinator.max.retries");
    private static final int MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS =
            DmeConfig.getInt("node.coordinator.max.unavailable");
    private static final int TOKEN_WAIT_TIMEOUT_MS = DmeConfig.getInt("node.token.wait.timeout.ms");
    private static final int CRITICAL_SECTION_MIN_MS = DmeConfig.getInt("node.critical-section.min.ms");
    private static final int CRITICAL_SECTION_JITTER_MS = DmeConfig.getInt("node.critical-section.jitter.ms");
    private static final int HEALTH_CHECK_INITIAL_DELAY_MS =
            DmeConfig.getInt("node.coordinator.health.check.initial.delay.ms");
    private static final int HEALTH_CHECK_INTERVAL_MS =
            DmeConfig.getInt("node.coordinator.health.check.interval.ms");
    private static final int HEALTH_CHECK_TIMEOUT_MS =
            DmeConfig.getInt("node.coordinator.health.check.timeout.ms");

    private boolean inCriticalSection = false;
    private Timer coordinatorCheckTimer;
    private int shutdownRetryCount = 0;
    private int coordinatorUnavailableCount = 0;

    /**
     * @param nam node hostname
     * @param por node port
     * @param sec maximum delay (ms) before requesting the token
     * @param priorityLevel priority (1=high, 2=medium, 3=low)
     * @param shutdownRequested whether this node will request a system shutdown
     */
    public Node(String nam, int por, int sec, int priorityLevel, boolean shutdownRequested) {
        ra = new Random();
        n_host_name = nam;
        n_port = por;
        this.priorityLevel = priorityLevel;
        this.shutdownRequested = shutdownRequested;

        String priorityInfo = getPriorityInfo();
        log.info("Node {}:{}{} is active", n_host_name, n_port, priorityInfo);

        startCoordinatorHealthCheck();

        while (true) {
            try {
                int sleepTime = ra.nextInt(sec * 2);
                log.debug("Node {}:{}{} sleeping for {}ms before requesting token",
                        n_host_name, n_port, priorityInfo, sleepTime);
                Thread.sleep(sleepTime);

                if (shutdownRequested && shutdownRetryCount >= MAX_SHUTDOWN_RETRIES) {
                    log.info("Node {}:{}{} stopping after {} shutdown retries",
                            n_host_name, n_port, priorityInfo, MAX_SHUTDOWN_RETRIES);
                    System.exit(0); // refactored in Phase 4
                }

                log.info("Node {}:{}{} requesting token from coordinator",
                        n_host_name, n_port, priorityInfo);

                if (!sendTokenRequest(shutdownRequested)) {
                    continue;
                }

                if (shutdownRequested) {
                    shutdownRequested = false;
                }

                log.debug("Node {}:{}{} waiting for token", n_host_name, n_port, priorityInfo);

                try {
                    n_ss = new ServerSocket(n_port);
                    n_ss.setSoTimeout(TOKEN_WAIT_TIMEOUT_MS);

                    try {
                        n_token = n_ss.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(n_token.getInputStream()));
                        String tokenMsg = in.readLine();
                        log.info("Node {}:{}{} received token: {}",
                                n_host_name, n_port, priorityInfo, tokenMsg);
                        n_token.close();
                    } catch (SocketTimeoutException e) {
                        log.warn("Node {}:{}{} timeout waiting for token — coordinator could be down",
                                n_host_name, n_port, priorityInfo);
                        n_ss.close();
                        continue;
                    }

                    n_ss.close();
                } catch (IOException e) {
                    log.error("Node {}:{}{} error waiting for token",
                            n_host_name, n_port, priorityInfo, e);
                    continue;
                }

                inCriticalSection = true;
                log.info("Node {}:{}{} entering critical section", n_host_name, n_port, priorityInfo);
                int criticalSectionTime =
                        CRITICAL_SECTION_MIN_MS + ra.nextInt(CRITICAL_SECTION_JITTER_MS);
                Thread.sleep(criticalSectionTime);
                log.info("Node {}:{}{} exiting critical section after {}ms",
                        n_host_name, n_port, priorityInfo, criticalSectionTime);
                inCriticalSection = false;

                log.info("Node {}:{}{} returning token to coordinator",
                        n_host_name, n_port, priorityInfo);
                if (!returnToken()) {
                    log.warn("Node {}:{}{} failed to return token after multiple attempts",
                            n_host_name, n_port, priorityInfo);
                }

            } catch (InterruptedException e) {
                log.error("Node {}:{} interrupted", n_host_name, n_port, e);
                Thread.currentThread().interrupt();
                System.exit(1); // refactored in Phase 4
            }
        }
    }

    /**
     * Sends a token request to the Coordinator. Retries up to {@link #MAX_RETRIES} times.
     *
     * @param withShutdown whether to include the shutdown flag in the request
     * @return true if the request was sent successfully
     */
    private boolean sendTokenRequest(boolean withShutdown) {
        String priorityInfo = getPriorityInfo();
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                s = new Socket(c_host, c_request_port);
                pout = new PrintWriter(s.getOutputStream(), true);
                pout.println(n_host_name);
                pout.println(n_port);
                pout.println(priorityLevel);
                pout.println(withShutdown);

                log.debug("Node {}:{}{} sent request (priority={}, shutdown={})",
                        n_host_name, n_port, priorityInfo, priorityLevel, withShutdown);

                s.close();
                if (withShutdown) {
                    shutdownRetryCount++;
                    if (shutdownRetryCount >= MAX_SHUTDOWN_RETRIES) {
                        log.info("Node {}:{} initiating full system shutdown", n_host_name, n_port);
                        System.exit(0); // refactored in Phase 4
                    }
                }
                return true;
            } catch (IOException e) {
                coordinatorUnavailableCount++;
                log.warn("Node {}:{}{} failed to connect to coordinator ({}/{}): {}",
                        n_host_name, n_port, priorityInfo,
                        coordinatorUnavailableCount, MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS,
                        e.getMessage());

                if (coordinatorUnavailableCount >= MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS) {
                    log.error("Node {}:{}{} terminating due to coordinator failure",
                            n_host_name, n_port, priorityInfo);
                    System.exit(0); // refactored in Phase 4
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
     * Returns the token to the Coordinator. Retries on failure.
     */
    private boolean returnToken() {
        String priorityInfo = getPriorityInfo();
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try (Socket returnSocket = new Socket(c_host, c_return_port);
                 PrintWriter returnOut = new PrintWriter(returnSocket.getOutputStream(), true)) {
                returnOut.println("TOKEN RETURNED FROM " + n_host_name + ":" + n_port + priorityInfo);
                log.info("Node {}:{}{} token returned successfully", n_host_name, n_port, priorityInfo);
                return true;
            } catch (IOException e) {
                log.warn("Node {}:{}{} failed to return token (attempt {}/{}): {}",
                        n_host_name, n_port, priorityInfo, attempt + 1, MAX_RETRIES, e.getMessage());

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

    private void startCoordinatorHealthCheck() {
        coordinatorCheckTimer = new Timer("CoordinatorChecker", true);
        coordinatorCheckTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                checkCoordinatorHealth();
            }
        }, HEALTH_CHECK_INITIAL_DELAY_MS, HEALTH_CHECK_INTERVAL_MS);
    }

    private void checkCoordinatorHealth() {
        if (inCriticalSection || shutdownRequested) {
            return;
        }

        String priorityInfo = getPriorityInfo();
        try (Socket healthCheckSocket = new Socket()) {
            healthCheckSocket.connect(new InetSocketAddress(c_host, c_request_port), HEALTH_CHECK_TIMEOUT_MS);
            coordinatorUnavailableCount = 0;
        } catch (IOException e) {
            coordinatorUnavailableCount++;
            log.warn("Node {}:{}{} detected coordinator unresponsive ({}/{}): {}",
                    n_host_name, n_port, priorityInfo,
                    coordinatorUnavailableCount, MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS,
                    e.getMessage());

            if (coordinatorUnavailableCount >= MAX_COORDINATOR_UNAVAILABLE_ATTEMPTS) {
                log.error("Node {}:{}{} terminating due to coordinator failure",
                        n_host_name, n_port, priorityInfo);
                System.exit(0); // refactored in Phase 4
            }
        }
    }

    /**
     * @param args [0] node port, [1] max delay (ms), [2] priority (1-3, optional),
     *             [3] whether to request shutdown (true/false, optional)
     */
    public static void main(String args[]) {
        String n_host_name = "";
        int n_port;
        boolean shutdownRequested = false;

        int priorityLevel = 3;
        if (args.length >= 3) {
            priorityLevel = Integer.parseInt(args[2]);
            if (priorityLevel < 1 || priorityLevel > 3) {
                log.error("Priority must be 1 (high), 2 (medium), or 3 (low)");
                System.exit(1);
            }
        }

        try {
            InetAddress n_inet_address = InetAddress.getLocalHost();
            n_host_name = n_inet_address.getHostName();
        } catch (java.net.UnknownHostException e) {
            log.error("Failed to resolve local host", e);
            System.exit(1);
        }

        n_port = Integer.parseInt(args[0]);

        if (args.length == 4) {
            shutdownRequested = Boolean.parseBoolean(args[3]);
        }

        log.info("Starting node on {}:{} (priority={}, shutdown={})",
                n_host_name, n_port, priorityLevel, shutdownRequested);

        new Node(n_host_name, n_port, Integer.parseInt(args[1]), priorityLevel, shutdownRequested);
    }

    private String getPriorityInfo() {
        switch (priorityLevel) {
            case 1: return " (HIGH PRIORITY)";
            case 2: return " (MEDIUM PRIORITY)";
            case 3: return " (LOW PRIORITY)";
            default: return " (UNKNOWN PRIORITY)";
        }
    }
}
