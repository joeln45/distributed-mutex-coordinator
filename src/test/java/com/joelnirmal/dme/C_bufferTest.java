package com.joelnirmal.dme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link C_buffer}. The anti-starvation threshold is overridden
 * to 100ms via src/test/resources/application.properties, so the starvation
 * test takes ~150ms instead of >10s.
 */
class C_bufferTest {

    private static String[] req(String host, int port, int priority) {
        return new String[]{host, String.valueOf(port), String.valueOf(priority)};
    }

    @Test
    void fifoOrderWithinSamePriority() {
        C_buffer buffer = new C_buffer();
        buffer.saveRequest(req("a", 8001, 2));
        buffer.saveRequest(req("b", 8002, 2));
        buffer.saveRequest(req("c", 8003, 2));

        assertEquals("a", buffer.getNext().getHost());
        assertEquals("b", buffer.getNext().getHost());
        assertEquals("c", buffer.getNext().getHost());
    }

    @Test
    void higherPriorityServedFirst() {
        C_buffer buffer = new C_buffer();
        // Insert out of priority order to prove sorting, not insertion luck.
        buffer.saveRequest(req("low", 8001, 3));
        buffer.saveRequest(req("high", 8002, 1));
        buffer.saveRequest(req("mid", 8003, 2));

        assertEquals("high", buffer.getNext().getHost());
        assertEquals("mid", buffer.getNext().getHost());
        assertEquals("low", buffer.getNext().getHost());
    }

    @Test
    void antiStarvationPromotesLowPriorityAfterThreshold() throws InterruptedException {
        C_buffer buffer = new C_buffer();
        buffer.saveRequest(req("low", 8001, 3));

        // Sleep past the test threshold (100ms) so the low-priority request
        // becomes eligible for anti-starvation promotion.
        Thread.sleep(200);

        // Adding a new request triggers re-sort; the waiting low-prio item
        // should be promoted above the freshly-added high-prio one.
        buffer.saveRequest(req("high", 8002, 1));

        assertEquals("low", buffer.getNext().getHost(),
                "starved low-priority request should be promoted ahead of fresh high-priority");
        assertEquals("high", buffer.getNext().getHost());
    }

    @Test
    void getNextReturnsNullAfterShutdownWhenEmpty() {
        C_buffer buffer = new C_buffer();
        buffer.initiateShutdown();
        assertNull(buffer.getNext(), "getNext should return null once shutdown is initiated and buffer is empty");
    }

    @Test
    void invalidPriorityDefaultsToLow() {
        C_buffer buffer = new C_buffer();
        buffer.saveRequest(req("a", 8001, 99)); // out of range
        buffer.saveRequest(new String[]{"b", "8002", "not-a-number"}); // non-numeric

        RequestItem first = buffer.getNext();
        RequestItem second = buffer.getNext();
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(3, first.getPriorityLevel());
        assertEquals(3, second.getPriorityLevel());
    }

    @Test
    void sizeReflectsAddsAndRemoves() {
        C_buffer buffer = new C_buffer();
        assertEquals(0, buffer.size());
        buffer.saveRequest(req("a", 8001, 1));
        buffer.saveRequest(req("b", 8002, 1));
        assertEquals(2, buffer.size());
        buffer.getNext();
        assertEquals(1, buffer.size());
    }
}
