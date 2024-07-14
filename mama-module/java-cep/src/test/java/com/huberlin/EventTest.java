package com.huberlin;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import static  com.huberlin.event.Event.parseTimestamp;
import static  com.huberlin.event.Event.formatTimestamp;
import static  com.huberlin.event.Event.getPrimitiveTypes;
import static org.junit.jupiter.api.Assertions.*;

class EventTest {

    @Test
    void testGetPrimitiveTypes() {
        assertEquals(new HashSet<>(Arrays.asList("A", "B", "C", "D")),getPrimitiveTypes("AND(A,B,SEQ(C,D))"));
        assertEquals(new HashSet<>(Arrays.asList("B")), getPrimitiveTypes("B"));
        assertThrows(IllegalArgumentException.class, () -> {getPrimitiveTypes("");});
        assertThrows(IllegalArgumentException.class, () -> {getPrimitiveTypes("SEQ (A,B,C)");});
        assertThrows(IllegalArgumentException.class, () -> {getPrimitiveTypes("SEQ(A,AND(C,D,C)");});
    }

    @Test
    public void testSimpleFunction() {
        HashSet<String> expected = new HashSet<>();
        expected.add("A");
        expected.add("B");

        assertEquals(expected, getPrimitiveTypes("AND(A,B)"));
    }

    @Test
    public void testNestedFunctions() {
        HashSet<String> expected = new HashSet<>();
        expected.add("A");
        expected.add("B");
        expected.add("C");
        expected.add("D");
        expected.add("E");

        assertEquals(expected, getPrimitiveTypes("SEQ(A,AND(B,C),SEQ(D,E))"));
    }

    @Test
    public void testParseAndFormatTimestamp() {
        String timestampString = "01:23:45:678901";
        long timestampLong = 5025678901L;
        assertEquals(timestampLong, parseTimestamp(timestampString));
        assertEquals(timestampString, formatTimestamp(timestampLong));

        // Test for a timestamp of 00:00:00:000000
        timestampString = "00:00:00:000000";
        timestampLong = 0L;
        assertEquals(timestampLong, parseTimestamp(timestampString));
        assertEquals(timestampString, formatTimestamp(timestampLong));

        timestampString = "23:59:59:999999";
        timestampLong = 86399999999L;
        assertEquals(timestampLong, parseTimestamp(timestampString));
        assertEquals(timestampString, formatTimestamp(timestampLong));

        assertThrows(java.time.DateTimeException.class, () -> {formatTimestamp(86399999999L + 1);}); //23:59:59:999999 + 1
        //previous code only avoided this exception by using the strig form wherever possible, which simply led to decreasing timestamps at midnight
        //so technically all our timestamps are modulo o86400000000 ...

    }

}