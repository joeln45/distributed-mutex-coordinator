package com.joelnirmal.dme;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class provides utility methods for logging messages to a file.
 * It supports appending log entries with timestamps and clearing the log file.
 * The log file is named "Output.txt" and is located in the application's working directory.
 */
public class Logger {
    private static final String LOG_FILE = "Output.txt";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Clears the log file by deleting it and creating a new empty file.
     * This method ensures that the log file is reset to an empty state.
     * 
     * If an error occurs during the process, it is printed to the standard error stream.
     */
    public static synchronized void clearLog() {
        try {
            new File(LOG_FILE).delete();
            // Create a new empty file
            new File(LOG_FILE).createNewFile();
        } catch (IOException e) {
            System.err.println("Error clearing log file: " + e.getMessage());
        }
    }

    /**
     * Logs a message to the log file with a timestamp and source identifier.
     * Each log entry is written in the format:
     * [timestamp] [source] message
     * 
     * @param source  The source of the log message, typically the class or module name.
     * @param message The message to be logged.
     * 
     * If an error occurs while writing to the log file, it is printed to the standard error stream.
     */
    public static synchronized void log(String source, String message) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE, true))) {
            String timestamp = dateFormat.format(new Date());
            writer.println("[" + timestamp + "] [" + source + "] " + message);
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
    }
}