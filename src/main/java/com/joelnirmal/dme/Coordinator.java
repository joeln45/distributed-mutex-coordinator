package com.joelnirmal.dme;

import java.net.InetAddress;

import org.slf4j.LoggerFactory;

/**
 * This class initializes the system, starts the necessary threads (receiver and mutex),
 * and handles graceful shutdown.
 *
 * <p>The Coordinator listens for incoming requests, manages token granting, and ensures
 * proper cleanup during shutdown.</p>
 */
public class Coordinator {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Coordinator.class);

    /**
     * The main method initializes the Coordinator, starts the necessary threads,
     * and sets up a shutdown hook.
     *
     * @param args Command-line arguments. Optionally, the first argument can specify the port number.
     */
    public static void main(String args[]) {
        int port = DmeConfig.getInt("coordinator.request.port");

        try {
            InetAddress c_addr = InetAddress.getLocalHost();
            String c_name = c_addr.getHostName();
            log.info("Coordinator is starting at {} ({})", c_name, c_addr);
        } catch (Exception e) {
            log.error("Failed to resolve coordinator host", e);
        }

        // Allow the port to be specified as a command-line argument
        if (args.length == 1) port = Integer.parseInt(args[0]);

        C_buffer buffer = new C_buffer();

        C_receiver receiver = new C_receiver(buffer, port);
        receiver.start();

        C_mutex mutex = new C_mutex(buffer, port);
        mutex.start();

        log.info("Coordinator started and running on port {}", port);

        // Add a shutdown hook to handle graceful shutdown of the Coordinator
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutting down coordinator");

                receiver.requestShutdown();
                mutex.requestShutdown();

                try {
                    receiver.join();
                    mutex.join();
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for threads to finish");
                    Thread.currentThread().interrupt();
                }

                log.info("Coordinator shutdown completed");
            }
        });
    }
}
