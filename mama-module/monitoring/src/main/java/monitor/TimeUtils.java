// package monitor;
package com.huberlin.monitor;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

// TODO: put into shared module
public class TimeUtils {
  public static String format(long microseconds) {
    long HH = TimeUnit.MICROSECONDS.toHours(microseconds);
    long MM = TimeUnit.MICROSECONDS.toMinutes(microseconds) % 60;
    long SS = TimeUnit.MICROSECONDS.toSeconds(microseconds) % 60;
    long MS =
        TimeUnit.MICROSECONDS.toMillis(microseconds)
            % 1000; // Convert remaining microseconds to milliseconds
    long US = microseconds % 1000; // Remaining microseconds

    return String.format("%02d:%02d:%02d.%03d%03d", HH, MM, SS, MS, US);
  }

  public static Long getCurrentTimeInMicroseconds() {
    // Get the current time
    LocalTime now = LocalTime.now();

    // Calculate the total microseconds since the start of the day
    long hoursInMicroseconds = now.getHour() * 60L * 60L * 1_000_000L;
    long minutesInMicroseconds = now.getMinute() * 60L * 1_000_000L;
    long secondsInMicroseconds = now.getSecond() * 1_000_000L;
    long nanoInMicroseconds = now.getNano() / 1_000L;

    return hoursInMicroseconds + minutesInMicroseconds + secondsInMicroseconds + nanoInMicroseconds;
  }
}
