package com.huberlin.javacep;

import static org.junit.jupiter.api.Assertions.*;

import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import java.time.LocalTime;
import java.util.*;
import org.junit.jupiter.api.Test;

class GeneralTest {

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

    ComplexEvent complexEvent =
        new ComplexEvent(getCurrentTimeInMicroseconds(), "_", simpleEvents, null);

    System.out.println(complexEvent.toString());
    System.out.println("complexEvent.multiSinkQueryEnabled: " + complexEvent.multiSinkQueryEnabled);
    assertFalse(complexEvent.multiSinkQueryEnabled);
  }

  @Test
  void flushedAttributeTest() {
    SimpleEvent simpleEvent1 = (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D");
    System.out.println(simpleEvent1.attributeList);
    SimpleEvent simpleEvent2 =
        (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D | true");
    SimpleEvent simpleEvent3 =
        (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D | false");
    ComplexEvent ce =
        (ComplexEvent)
            Event.parse(
                "complex | 21:23:44:253694 | SEQ(F, E) | 2 | (21:23:09:000000, 374735, F, true)"
                    + " ;(21:23:44:000000, f9bb6169, E, true) | true | (21:23:09:000000, 374735, F,"
                    + " true) ;(21:23:44:000000, f9bb6169, E, true) | true");

    System.out.println("Before adding the attribute 'flushed': " + simpleEvent1.toString());
    simpleEvent1.addAttribute("flushed");
    System.out.println("After adding the attribute 'flushed': " + simpleEvent1.toString());

    System.out.println("Before adding the attribute 'flushed': " + ce.toString());
    ce.addAttribute("flushed");
    System.out.println("After adding the attribute 'flushed': " + ce.toString());

    System.out.println(
        "simpleEvent1.multiSinkQueryEnabled before change: " + simpleEvent1.multiSinkQueryEnabled);
    simpleEvent1.setMultiSinkQueryEnabled(false);
    System.out.println(
        "simpleEvent1.multiSinkQueryEnabled after change: " + simpleEvent1.multiSinkQueryEnabled);

    System.out.println(
        "simpleEvent2.multiSinkQueryEnabled before change: " + simpleEvent2.multiSinkQueryEnabled);
    simpleEvent2.setMultiSinkQueryEnabled(false);
    System.out.println(
        "simpleEvent2.multiSinkQueryEnabled after change: " + simpleEvent2.multiSinkQueryEnabled);

    System.out.println(simpleEvent3.multiSinkQueryEnabled);

    System.out.println(
        "simpleEvent3.multiSinkQueryEnabled before change: " + simpleEvent3.multiSinkQueryEnabled);
    simpleEvent3.setMultiSinkQueryEnabled(true);
    System.out.println(
        "simpleEvent3.multiSinkQueryEnabled after change: " + simpleEvent3.multiSinkQueryEnabled);
  }

  @Test
  void createComplexEventfromSimpleTest() {

    SimpleEvent D = (SimpleEvent) Event.parse("simple | c46c099b | 18:38:45:000000 | D");
    SimpleEvent A = (SimpleEvent) Event.parse("simple | c46c098b | 18:38:46:000000 | A | false");
    // A.setMultiSinkQueryEnabled(false);
    ArrayList<SimpleEvent> eventList = new ArrayList<>();
    eventList.add(D);
    eventList.add(A);
    ComplexEvent ce =
        new ComplexEvent(getCurrentTimeInMicroseconds(), "SEQ(D, A)", eventList, new ArrayList<>());
    System.out.println(ce.toString());
    System.out.println(ce.getEventType());
  }
}
