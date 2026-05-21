package com.joelnirmal.dme;

import java.util.*;

/**
 * A shared buffer used by the Coordinator to store and manage
 * token requests from nodes. It supports priority-based sorting with anti-starvation logic.
 */
public class C_buffer {

    private List<RequestItem> data; // Stores token requests
    private final long lowPriorityWaitThreshold =
            DmeConfig.getLong("coordinator.anti-starvation.threshold.ms");
    private boolean shutdownInitiated = false; // Indicates if shutdown has been initiated

    /**
     * Constructor.
     * Initializes an empty list to store the token requests.
     */
    public C_buffer() {
        data = new ArrayList<RequestItem>();
    }

    /**
     * Gets the current size of the buffer.
     *
     * @return The number of requests currently in the buffer.
     */
    public int size() {
        return data.size();
    }

    /**
     * Creates a new RequestItem from the provided data, adds it to the buffer,
     * and sorts the buffer using anti-starvation logic.
     *
     * @param r An array containing request details - host, port, and priority.
     */
    public synchronized void saveRequest(String[] r) {
        int priorityLevel = 3; // Default to low priority
        if (r.length > 2) {
            try {
                priorityLevel = Integer.parseInt(r[2]);
                // Ensure priority is within the valid range (1-3)
                if (priorityLevel < 1 || priorityLevel > 3) {
                    priorityLevel = 3; // Default to low if invalid
                }
            } catch (NumberFormatException e) {
                priorityLevel = 3; // Default to low if parsing fails
            }
        }

        // Create a new token request with priority level and timestamp
        RequestItem request = new RequestItem(r[0], r[1], priorityLevel);

        // Add to the list and sort based on priority and waiting time
        data.add(request);
        sortWithAntiStarvation();

        String priorityInfo;
        switch (priorityLevel) {
            case 1: priorityInfo = " (HIGH PRIORITY)"; break;
            case 2: priorityInfo = " (MEDIUM PRIORITY)"; break;
            case 3: priorityInfo = " (LOW PRIORITY)"; break;
            default: priorityInfo = " (UNKNOWN PRIORITY)";
        }

        // Log the buffer size after adding a request
        Logger.log("Coordinator-Buffer", "Request added from " + r[0] + ":" + r[1] +
                priorityInfo + ", new queue length: " + data.size());

        // Notify any waiting threads that data is available
        notifyAll();
    }

    /**
     * Sorts the buffer using anti-starvation logic.
     * Requests are sorted based on priority, and low-priority requests that have waited too long
     * (approximately 10 seconds) are prioritized to prevent starvation.
     */
    private void sortWithAntiStarvation() {
        long currentTime = System.currentTimeMillis();

        Collections.sort(data, new Comparator<RequestItem>() {
            @Override
            public int compare(RequestItem r1, RequestItem r2) {
                long r1WaitTime = currentTime - r1.getTimestamp();
                long r2WaitTime = currentTime - r2.getTimestamp();

                // Anti-starvation: prioritize low-priority requests waiting too long
                if (r1.getPriorityLevel() == 3 && r1WaitTime > lowPriorityWaitThreshold) {
                    if (r2.getPriorityLevel() == 3 && r2WaitTime > lowPriorityWaitThreshold) {
                        return Long.compare(r1.getTimestamp(), r2.getTimestamp());
                    }
                    return -1;
                }
                if (r2.getPriorityLevel() == 3 && r2WaitTime > lowPriorityWaitThreshold) {
                    return 1;
                }

                // Normal priority comparison (lower number = higher priority)
                if (r1.getPriorityLevel() != r2.getPriorityLevel()) {
                    return Integer.compare(r1.getPriorityLevel(), r2.getPriorityLevel());
                }

                // FIFO order for same priority
                return Long.compare(r1.getTimestamp(), r2.getTimestamp());
            }
        });

        // Log if any low-priority requests are prioritized due to long waiting time
        for (RequestItem item : data) {
            long waitTime = currentTime - item.getTimestamp();
            if (item.getPriorityLevel() == 3 && waitTime > lowPriorityWaitThreshold) {
                Logger.log("Coordinator-Buffer", "Anti-starvation: Low priority request from " +
                        item.getHost() + ":" + item.getPort() +
                        " prioritized after waiting " + (waitTime / 1000) + " seconds");
            }
        }
    }

    /**
     * Displays the current state of the buffer.
     * Prints all requests in the buffer to the console.
     */
    public void show() {
        for (RequestItem item : data) {
            System.out.print(" " + item + " ");
        }
        System.out.println(" ");
    }

    /**
     * Initiates a shutdown of the buffer.
     * This method sets the shutdown flag to true and notifies all waiting threads.
     */
    public synchronized void initiateShutdown() {
        shutdownInitiated = true;
        Logger.log("Coordinator-Buffer", "SHUTDOWN initiated");
        notifyAll();
    }

    /**
     * Checks if a shutdown has been initiated.
     *
     * @return True if a shutdown has been initiated, false otherwise.
     */
    public synchronized boolean isShutdownInitiated() {
        return shutdownInitiated;
    }

    /**
     * Retrieves the next RequestItem from the buffer.
     * If the buffer is empty and no shutdown has been initiated, this method waits.
     * 
     * @return The next RequestItem from the buffer, or null if the buffer is empty and shutdown is initiated.
     */
    public synchronized RequestItem getNext() {
        while (data.size() == 0 && !shutdownInitiated) {
            try {
                Logger.log("Coordinator-Buffer", "Buffer empty, waiting for requests");
                wait();
            } catch (InterruptedException e) {
                System.out.println("Error: " + e);
                Logger.log("Coordinator-Buffer", "Error: " + e.getMessage());
            }
        }

        if (shutdownInitiated && data.size() == 0) {
            return null;
        }

        return data.remove(0);
    }

    /**
     * Retrieves the host of the next request in the buffer.
     * 
     * @return The hostname of the next request, or null if the buffer is empty.
     */
    public synchronized Object get() {
        if (data.size() == 0) {
            return null;
        }

        RequestItem item = data.remove(0);
        return item.getHost();
    }
}