package com.huberlin.coordinator;

import static org.junit.jupiter.api.Assertions.*;

import com.huberlin.event.ControlEvent;
import com.huberlin.monitor.TimeUtils;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.LocalTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CoordinatorTest {

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

  public static Long addSeconds(Long timestamp, int seconds) {
    return timestamp + seconds * 1_000_000L;
  }

  @Test
  void testReceivingAlert() {
    Thread serverThread =
        new Thread(
            () -> {
              System.out.println("Starting Coordinator service...");
              Coordinator.main(
                  new String[] {
                    "-addressBook",
                    "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json"
                  });
            });
    serverThread.start();
    System.out.println("Sending alert to Coordinator");

    try {
      Thread.sleep(9000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Long currentTime = getCurrentTimeInMicroseconds();
    ControlEvent controlEvent = new ControlEvent(Optional.of(currentTime), Optional.empty());
    try {
      Socket clientSocket = new Socket("localhost", 6668);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
      out.println(controlEvent.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void timestampParsingTest() {
    Long driftTime = getCurrentTimeInMicroseconds();
    ControlEvent controlEvent = new ControlEvent(Optional.of(driftTime), Optional.empty());
    System.out.println("controlEvent = " + controlEvent.toString());
    ControlEvent parsedControlEvent = ControlEvent.parse(controlEvent.toString()).get();
    System.out.println("parsedControlEvent = " + controlEvent.toString());
    assertEquals(controlEvent.toString(), parsedControlEvent.toString());

    Long newTime = addSeconds(driftTime, 5);

    assert (newTime - driftTime == 5 * 1_000_000L);
    System.out.println("newTime = " + TimeUtils.format(newTime));
  }
}
