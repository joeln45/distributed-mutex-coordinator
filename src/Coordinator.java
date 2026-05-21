import java.net.*;

/**
 * This class initializes the system, starts the necessary threads (receiver and mutex), and handles graceful shutdown.
 * The Coordinator listens for incoming requests, manages token granting, and ensures proper cleanup during shutdown.
 */
public class Coordinator {

    /**
     * The main method initializes the Coordinator, starts the necessary threads, and sets up a shutdown hook.
     * 
     * @param args Command-line arguments. Optionally, the first argument can specify the port number.
     */
    public static void main(String args[]) {
        // Clear the log file at the start of the program
        Logger.clearLog();
        int port = 7000; // Default port for the Coordinator

        // Create an instance of the Coordinator
        Coordinator c = new Coordinator();

        try {
            // Get the local host address and name
            InetAddress c_addr = InetAddress.getLocalHost();
            String c_name = c_addr.getHostName();

            // Print and log the Coordinator's address and hostname
            System.out.println("Coordinator address is " + c_addr);
            System.out.println("Coordinator host name is " + c_name + "\n\n");
            Logger.log("Coordinator-Main", "Coordinator is starting at " + c_name + " (" + c_addr + ")");
        } catch (Exception e) {
            // Handle any errors while retrieving the host information
            System.err.println(e);
            System.err.println("Error in coordinator");
            Logger.log("Coordinator-Main", "error: " + e.getMessage());
        }

        // Allow the port to be specified as a command-line argument
        if (args.length == 1) port = Integer.parseInt(args[0]);

        // Create a shared buffer for communication between threads
        C_buffer buffer = new C_buffer();

        // Start the receiver thread to handle incoming requests
        C_receiver receiver = new C_receiver(buffer, port);
        receiver.start();

        // Start the mutex thread to manage token granting
        C_mutex mutex = new C_mutex(buffer, port);
        mutex.start();

        // Log and print that the Coordinator has started
        System.out.println("Coordinator started and running...");
        Logger.log("Coordinator-Main", "Coordinator started and is now running at the port " + port);

        // Add a shutdown hook to handle graceful shutdown of the Coordinator
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("\nCoordinator is now shutting down...");
                Logger.log("Coordinator-Main", "Shutting down coordinator");

                // Request shutdown of the receiver and mutex threads
                receiver.requestShutdown();
                mutex.requestShutdown();

                try {
                    // Wait for the threads to finish their tasks
                    receiver.join();
                    mutex.join();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted while waiting for threads to finish");
                }

                // Log and print that the shutdown is complete
                System.out.println("Coordinator shutdown completed");
                Logger.log("Coordinator-Main", "Coordinator shutdown completed");
            }
        });
    }
}