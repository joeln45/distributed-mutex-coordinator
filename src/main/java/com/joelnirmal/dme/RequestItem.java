package com.joelnirmal.dme;

/**
 * Thhis class represents a request in the distributed mutual exclusion system.
 * Each request contains information about the requesting node, its priority, and the time it was created.
 * This class is used to manage and prioritize requests in the Coordinator's buffer.
 */
class RequestItem {
    private String host; // The hostname of the requesting node
    private String port; // The port number of the requesting node
    private int priorityLevel; // Priority level (1=high, 2=medium, 3=low)
    private long timestamp; // The time when the request was created (in milliseconds)

    /**
     * Constructor for the RequestItem class.
     * Initializes the request with the host, port, priority level, and records the creation timestamp.
     *
     * @param host The hostname of the requesting node.
     * @param port The port number of the requesting node.
     * @param priorityLevel The priority level of the request (1=high, 2=medium, 3=low).
     */
    public RequestItem(String host, String port, int priorityLevel) {
        this.host = host;
        this.port = port;
        this.priorityLevel = priorityLevel;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Gets the hostname of the requesting node.
     *
     * @return The hostname of the node that made the request.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number of the requesting node.
     *
     * @return The port number of the node that made the request.
     */
    public String getPort() {
        return port;
    }

    /**
     * Gets the priority level of the request.
     *
     * @return The priority level (1=high, 2=medium, 3=low).
     */
    public int getPriorityLevel() {
        return priorityLevel;
    }

    /**
     * Gets the timestamp of when the request was created.
     *
     * @return The timestamp (in milliseconds) of the request's creation time.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Converts the request to a human-readable string format.
     * Includes the hostname, port, priority level, and the time the request has been waiting.
     *
     * @return A string representation of the request.
     */
    @Override
    public String toString() {
        String priorityStr;
        switch (priorityLevel) {
            case 1: priorityStr = " (HIGH PRIORITY)"; break;
            case 2: priorityStr = " (MEDIUM PRIORITY)"; break;
            case 3: priorityStr = " (LOW PRIORITY)"; break;
            default: priorityStr = " (UNKNOWN PRIORITY)";
        }
        return host + ":" + port + priorityStr + " [waiting: " + 
               ((System.currentTimeMillis() - timestamp) / 1000) + "s]";
    }
}