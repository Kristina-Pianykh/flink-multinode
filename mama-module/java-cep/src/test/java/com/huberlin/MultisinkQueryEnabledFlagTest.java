package com.huberlin.javacep;

import static org.junit.jupiter.api.Assertions.*;

import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import java.time.LocalTime;
import java.util.*;
import org.junit.jupiter.api.Test;

class MultisinkQueryEnabledFlagTest {

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

  @Test
  void flagForComplexEventsTest() {
    SimpleEvent simpleEvent1 = (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D");
    simpleEvent1.setMultiSinkQueryEnabled(false);
    SimpleEvent simpleEvent2 = (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D");
    simpleEvent2.setMultiSinkQueryEnabled(true);
    SimpleEvent simpleEvent3 = (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D");
    simpleEvent3.setMultiSinkQueryEnabled(true);

    ArrayList<SimpleEvent> simpleEvents = new ArrayList<>();
    simpleEvents.add(simpleEvent1);
    simpleEvents.add(simpleEvent2);
    simpleEvents.add(simpleEvent3);

    ComplexEvent complexEvent = new ComplexEvent(getCurrentTimeInMicroseconds(), "_", simpleEvents);

    System.out.println(complexEvent.toString());
    System.out.println("complexEvent.multiSinkQueryEnabled: " + complexEvent.multiSinkQueryEnabled);
    assertFalse(complexEvent.multiSinkQueryEnabled);
  }
}
