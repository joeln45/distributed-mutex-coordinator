package com.joelnirmal.dme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RequestItem}. RequestItem is a passive value object —
 * it stores whatever it's given. Priority *defaulting* for out-of-range values
 * happens at the C_buffer layer, so that behaviour is tested in C_bufferTest.
 */
class RequestItemTest {

    @Test
    void constructorStoresFields() {
        RequestItem r = new RequestItem("node-a", "8001", 2);
        assertEquals("node-a", r.getHost());
        assertEquals("8001", r.getPort());
        assertEquals(2, r.getPriorityLevel());
    }

    @Test
    void timestampIsSetToNow() {
        long before = System.currentTimeMillis();
        RequestItem r = new RequestItem("node-a", "8001", 1);
        long after = System.currentTimeMillis();

        assertTrue(r.getTimestamp() >= before,
                "timestamp should be >= time before construction");
        assertTrue(r.getTimestamp() <= after,
                "timestamp should be <= time after construction");
    }

    @Test
    void toStringIncludesHostPortAndPriorityLabel() {
        assertTrue(new RequestItem("h", "1", 1).toString().contains("(HIGH PRIORITY)"));
        assertTrue(new RequestItem("h", "1", 2).toString().contains("(MEDIUM PRIORITY)"));
        assertTrue(new RequestItem("h", "1", 3).toString().contains("(LOW PRIORITY)"));
        assertTrue(new RequestItem("h", "1", 99).toString().contains("(UNKNOWN PRIORITY)"));
    }

    @Test
    void toStringStartsWithHostAndPort() {
        String s = new RequestItem("node-a", "8001", 2).toString();
        assertTrue(s.startsWith("node-a:8001"), "got: " + s);
    }
}
