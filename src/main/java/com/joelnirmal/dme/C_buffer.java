package com.joelnirmal.dme;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.slf4j.LoggerFactory;

/**
 * A shared buffer used by the Coordinator to store and manage
 * token requests from nodes. Supports priority-based sorting with anti-starvation logic.
 */
public class C_buffer {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(C_buffer.class);

    private List<RequestItem> data; // Stores token requests
    private final long lowPriorityWaitThreshold =
            DmeConfig.getLong("coordinator.anti-starvation.threshold.ms");
    private boolean shutdownInitiated = false;

    /**
     * Constructor. Initializes an empty list to store token requests.
     */
    public C_buffer() {
        data = new ArrayList<>();
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
                if (priorityLevel < 1 || priorityLevel > 3) {
                    priorityLevel = 3;
                }
            } catch (NumberFormatException e) {
                priorityLevel = 3;
            }
        }

        RequestItem request = new RequestItem(r[0], r[1], priorityLevel);

        data.add(request);
        sortWithAntiStarvation();

        log.info("Request added from {}:{}{}, new queue length: {}",
                r[0], r[1], priorityLabel(priorityLevel), data.size());

        notifyAll();
    }

    /**
     * Sorts the buffer using anti-starvation logic. Requests are sorted by priority;
     * low-priority requests waiting longer than the anti-starvation threshold are promoted.
     */
    private void sortWithAntiStarvation() {
        long currentTime = System.currentTimeMillis();

        Collections.sort(data, new Comparator<RequestItem>() {
            @Override
            public int compare(RequestItem r1, RequestItem r2) {
                long r1WaitTime = currentTime - r1.getTimestamp();
                long r2WaitTime = currentTime - r2.getTimestamp();

                if (r1.getPriorityLevel() == 3 && r1WaitTime > lowPriorityWaitThreshold) {
                    if (r2.getPriorityLevel() == 3 && r2WaitTime > lowPriorityWaitThreshold) {
                        return Long.compare(r1.getTimestamp(), r2.getTimestamp());
                    }
                    return -1;
                }
                if (r2.getPriorityLevel() == 3 && r2WaitTime > lowPriorityWaitThreshold) {
                    return 1;
                }

                if (r1.getPriorityLevel() != r2.getPriorityLevel()) {
                    return Integer.compare(r1.getPriorityLevel(), r2.getPriorityLevel());
                }

                return Long.compare(r1.getTimestamp(), r2.getTimestamp());
            }
        });

        for (RequestItem item : data) {
            long waitTime = currentTime - item.getTimestamp();
            if (item.getPriorityLevel() == 3 && waitTime > lowPriorityWaitThreshold) {
                log.info("Anti-starvation: low priority request from {}:{} prioritised after waiting {}s",
                        item.getHost(), item.getPort(), waitTime / 1000);
            }
        }
    }

    /**
     * Logs the current state of the buffer at DEBUG level.
     */
    public void show() {
        if (!log.isDebugEnabled()) return;
        StringBuilder sb = new StringBuilder("Buffer contents:");
        for (RequestItem item : data) {
            sb.append(' ').append(item);
        }
        log.debug(sb.toString());
    }

    /**
     * Initiates a shutdown of the buffer.
     * Sets the shutdown flag and notifies all waiting threads.
     */
    public synchronized void initiateShutdown() {
        shutdownInitiated = true;
        log.info("SHUTDOWN initiated");
        notifyAll();
    }

    /**
     * @return True if shutdown has been initiated, false otherwise.
     */
    public synchronized boolean isShutdownInitiated() {
        return shutdownInitiated;
    }

    /**
     * Retrieves the next RequestItem from the buffer.
     * Blocks until a request is available, or returns null if shutdown is initiated.
     */
    public synchronized RequestItem getNext() {
        while (data.size() == 0 && !shutdownInitiated) {
            try {
                log.debug("Buffer empty, waiting for requests");
                wait();
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for buffer", e);
                Thread.currentThread().interrupt();
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

    private static String priorityLabel(int level) {
        switch (level) {
            case 1: return " (HIGH PRIORITY)";
            case 2: return " (MEDIUM PRIORITY)";
            case 3: return " (LOW PRIORITY)";
            default: return " (UNKNOWN PRIORITY)";
        }
    }
}
