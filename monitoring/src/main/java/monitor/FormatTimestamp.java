package monitor;

import java.util.concurrent.TimeUnit;

public class FormatTimestamp {
  public static String format(long timestamp) {
    long HH = TimeUnit.MILLISECONDS.toHours(timestamp);
    long MM = TimeUnit.MILLISECONDS.toMinutes(timestamp) % 60;
    long SS = TimeUnit.MILLISECONDS.toSeconds(timestamp) % 60;
    return String.format("%02d:%02d:%02d", HH, MM, SS);
  }
}
