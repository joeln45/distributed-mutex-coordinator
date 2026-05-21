package com.joelnirmal.dme;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Centralised configuration loader for the DME system.
 *
 * <p>Reads {@code application.properties} from the classpath once at class load
 * and exposes typed accessors. Designed to fail fast — a missing file or a
 * missing key throws {@link IllegalStateException} so the system never silently
 * runs with stale defaults.</p>
 *
 * <p>This class is intentionally final with a private constructor; it is meant
 * to be used statically (e.g. {@code DmeConfig.getInt("coordinator.request.port")}).</p>
 */
public final class DmeConfig {

    private static final String CONFIG_FILE = "/application.properties";
    private static final Properties PROPS = load();

    private DmeConfig() {
        // utility class — not instantiable
    }

    private static Properties load() {
        Properties p = new Properties();
        try (InputStream in = DmeConfig.class.getResourceAsStream(CONFIG_FILE)) {
            if (in == null) {
                throw new IllegalStateException(
                        CONFIG_FILE + " not found on classpath. " +
                        "Ensure src/main/resources is included in the build.");
            }
            p.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load " + CONFIG_FILE, e);
        }
        return p;
    }

    /**
     * Returns the raw string value for the given key.
     *
     * @throws IllegalStateException if the key is not present.
     */
    public static String getString(String key) {
        String v = PROPS.getProperty(key);
        if (v == null) {
            throw new IllegalStateException("Missing config key: " + key);
        }
        return v.trim();
    }

    /** Returns the value parsed as an int. */
    public static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }

    /** Returns the value parsed as a long. */
    public static long getLong(String key) {
        return Long.parseLong(getString(key));
    }
}
