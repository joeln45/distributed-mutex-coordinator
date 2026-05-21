package com.joelnirmal.dme;

import org.slf4j.LoggerFactory;

/**
 * Thin compatibility shim over SLF4J.
 *
 * <p>Historically this project shipped a hand-rolled file logger that wrote
 * to a single {@code Output.txt} file (cleared on every coordinator restart).
 * That has been replaced by SLF4J + Logback — see {@code logback.xml} for the
 * rolling-file configuration that survives restarts.</p>
 *
 * <p>This class is kept so legacy callsites of the form
 * {@code Logger.log("Coordinator-Mutex", "...")} keep compiling. Internally,
 * each {@code source} string becomes the SLF4J logger name so log lines still
 * show the originating component.</p>
 *
 * <p>New code should prefer obtaining an SLF4J logger directly via
 * {@code LoggerFactory.getLogger(MyClass.class)} for class-name-based filtering.</p>
 */
public class Logger {

    private Logger() {
        // utility class — not instantiable
    }

    /**
     * Legacy no-op retained for backwards compatibility.
     *
     * <p>Logback handles log rotation now (see {@code logback.xml}), so there
     * is nothing to clear. Kept as a no-op so existing callers don't break.</p>
     */
    public static void clearLog() {
        // intentionally empty — Logback rolling policy supersedes manual clearing
    }

    /**
     * Logs an INFO-level message under the SLF4J logger named {@code source}.
     *
     * @param source  logger name (e.g. "Coordinator-Mutex"); used as the SLF4J logger name
     * @param message the message to log
     */
    public static void log(String source, String message) {
        LoggerFactory.getLogger(source).info(message);
    }
}
