package com.huberlin.event;

// package com.huberlin;

import java.io.Serializable;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Message implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  private static final Logger log = LoggerFactory.getLogger(Event.class);
  public final boolean control = false;

  // -------------------- Getter/Setter --------------------

  // set-methods should not be provided (effectively immutable object)
  public abstract long getTimestamp();

  /**
   * Serialize to string
   *
   * @return the unique string representation of this event
   */
  public abstract String toString();

  // -------------------- Helper functions, static, stateless  --------------------
  // --- Static methods for serialization (to string) and deserialization (from string) ---

  /**
   * Convert a string representation of an event to Event form
   *
   * @param received A event's unique string representation.
   * @return The event
   */

  // Timestamp functinos
  /*
   * As will become obvious our timestamps are really time-of-day timestamps, ranging from 0 to 24*3600*1_000_000 - 1 microseconds
   * Htis means nothing will work if the program starts before midnight and runs through midnight. Something to keep in mind.
   * This is not a problem I introduced, it was ther before. It does simplify things since the python sender assigns timestamps equal to the time of day, then waits for an offset specified in the trace, then adds that offset and sends,
   * also we compare timestamps to creation-timestamps which Steven decided to use LocalTime.now() for.
   *
   * All this can be fixed by changing to ISO timestamp format (YYYY-mm-ddThh:mm:ss.SSSSSS) as a serialization format instad of HH:MM:SS, but it adds to serialization overhead and fills up the screen
   * Will we really run experiments at night? ;)
   */
  public static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern(
          "HH:mm:ss:SSSSSS"); // Change this to YYYY-MM-ddTHH:mm:ss:SSSSSS to fix midnight problem

  public static String formatTimestamp(long timestamp) {
    LocalTime ts = LocalTime.ofNanoOfDay(timestamp * 1000L);
    return TIMESTAMP_FORMATTER.format(ts);
  }

  /**
   * Convert timestamp from long to string (serialized) form.
   *
   * @param timestamp numerical timestamp
   * @return timestamp in human-readable text form
   */
  // public static String formatTimestamp(long timestamp) {
  //   LocalTime ts = LocalTime.ofNanoOfDay(timestamp * 1000L);
  //   return TIMESTAMP_FORMATTER.format(ts);
  // }

  public static long parseTimestamp2(String serialized_timestamp) {
    LocalTime ts = LocalTime.parse(serialized_timestamp, TIMESTAMP_FORMATTER);
    return ts.toNanoOfDay() / 1000L;
  }

  /**
   * Parse serialized HH:mm:ss:SSSSSS timestamp. Note that it actually allows for HH to be 24 or
   * more, which is nice, but not enoug to solve the midnight issue If the formatter also emitted
   * such strings, it would solve it except for latency computation purposes. Latency computation
   * would still be broken if experiments ru across midnight
   *
   * @param hhmmssususus timestamp as string in form indicated by the name
   * @return timestamp as long
   */
  public static long parseTimestamp(String hhmmssususus) {
    String[] hoursMinutesSecondsMicroseconds = hhmmssususus.split(":");
    long resulting_timestamp = 0;
    assert (hoursMinutesSecondsMicroseconds.length >= 3);

    resulting_timestamp +=
        Long.parseLong(hoursMinutesSecondsMicroseconds[0], 10) * 60 * 60 * 1_000_000L; // fixed bug
    resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[1], 10) * 60 * 1_000_000L;
    resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[2], 10) * 1_000_000L;

    if (hoursMinutesSecondsMicroseconds.length == 4) {
      resulting_timestamp += Long.parseLong(hoursMinutesSecondsMicroseconds[3], 10);
    }
    return resulting_timestamp;
  }
}
