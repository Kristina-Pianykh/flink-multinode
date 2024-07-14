// package monitor;
package com.huberlin.monitor;

import java.util.concurrent.TimeUnit;

public class FormatTimestamp {
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
}
