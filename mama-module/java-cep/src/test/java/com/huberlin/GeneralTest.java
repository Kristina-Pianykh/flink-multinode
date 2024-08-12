package com.huberlin.javacep;

import static org.junit.jupiter.api.Assertions.*;

import com.huberlin.event.ComplexEvent;
import com.huberlin.event.ControlEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import com.huberlin.javacep.config.NodeConfig;
import java.io.IOException;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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
        new ComplexEvent(null, getCurrentTimeInMicroseconds(), "_", simpleEvents, null);

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
    ComplexEvent ce = null;
    ce = new ComplexEvent(null, getCurrentTimeInMicroseconds(), "SEQ(D, A)", eventList, null);
    System.out.println(ce.toString());

    ce =
        (ComplexEvent)
            Event.parse(
                "complex | dddd | 21:23:44:253694 | SEQ(F, E) | 2 | (21:23:09:000000, 374735, F,"
                    + " true) ;(21:23:44:000000, f9bb6169, E, true) | true | (21:23:09:000000,"
                    + " 374735, F, true) ;(21:23:44:000000, f9bb6169, E, true) | true");

    System.out.println(ce.toString());
  }

  @Test
  void scheduledTaskTest() {
    String filePath_local =
        "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs_dev/generate_flink_inputs/plans/config_0.json";
    String filePath_global =
        "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json";
    String rateMonitoringInputsPath =
        "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs_dev/generate_flink_inputs/plans/inequality_inputs.json";
    String updatedForwardingRulesPath =
        "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs_dev/generate_flink_inputs/plans/updated_forwared_rules.json";

    Set<Event> partEventBuffer = new HashSet<>();
    AtomicBoolean multiSinkQueryEnabled = new AtomicBoolean(true);

    NodeConfig config = new NodeConfig();
    try {
      config.parseJsonFile(
          filePath_local, filePath_global, rateMonitoringInputsPath, updatedForwardingRulesPath);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (config != null) {
      System.out.println("Parsed JSON successfully");
    } else {
      System.out.println("Failed to parse JSON");
      System.exit(1);
    }

    config.forwarding.table.get().print();

    // final int REST_PORT = 8081 + config.nodeId * 2;
    // Configuration flinkConfig =
    //     GlobalConfiguration.loadConfiguration(cmd.getOptionValue("flinkconfig", "conf"));
    // flinkConfig.set(JobManagerOptions.RPC_BIND_PORT, 6123 + config.nodeId);
    // flinkConfig.set(JobManagerOptions.PORT, 6123 + config.nodeId);
    // flinkConfig.set(RestOptions.BIND_PORT, REST_PORT + "-" + (REST_PORT + 1));
    // flinkConfig.set(RestOptions.PORT, REST_PORT);
    // flinkConfig.set(TaskManagerOptions.RPC_BIND_PORT, 51000 + config.nodeId);
    // flinkConfig.set(TaskManagerOptions.RPC_PORT, "0");
    // flinkConfig.set(BlobServerOptions.PORT, "0");
    //
    // StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // FileSystem.initialize(flinkConfig);
    //
    // // Only one engine-thread will work (output is displayed in the same way the packets arrive)
    // env.setParallelism(1);
    //
    // List<Integer> events = new ArrayList<>();
    // events.add(1);
    // events.add(2);
    // events.add(3);
    // DataStream<Integer> stream = env.fromCollection(events);

    // stream.addSink()

    try {
      ScheduledTask scheduledPartEventBufferFlush =
          new ScheduledTask(
              partEventBuffer,
              config.forwarding.table,
              config.forwarding.updatedTable,
              config.rateMonitoringInputs,
              config.nodeId,
              config.forwarding.addressBookTCP,
              multiSinkQueryEnabled);
      ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

      scheduledExecutorService.schedule(
          scheduledPartEventBufferFlush, 1, java.util.concurrent.TimeUnit.SECONDS);
      scheduledExecutorService.shutdown();
      try {
        Thread.sleep(9000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      config.forwarding.table.get().print();
    } catch (Exception e) {
      System.out.println("Failed to initialize a scheduled task");
      e.printStackTrace();
    }
  }

  @Test
  void instanceOf() {
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
    List<Event> eventList = List.of(simpleEvent1, simpleEvent2, simpleEvent3, ce);
    for (Event e : eventList) assert e instanceof Event;

    Optional<ControlEvent> controlEvent =
        ControlEvent.parse("control | 02:34:13:570350 | 02:34:21:570350");
    assert controlEvent.isPresent();
    assert controlEvent.get() instanceof ControlEvent;
  }

  @Test
  void eventIDComplexEvent() {
    ComplexEvent ce =
        (ComplexEvent)
            Event.parse(
                "complex | 38a8f1ak | 19:37:55:824436 | SEQ(F, D, E) | 3 | (19:37:36:000000,"
                    + " bf4778d4, D, true) ;(19:37:17:000000, 842511, F, true) ;(19:37:44:000000,"
                    + " f9bb6169, E, true) | true");
    System.out.println(ce.getID());
    List<String> simpleEventHashes = List.of("bf4778d4", "842511", "f9bb6169");
    // String concat = simpleEventHashes.stream().reduce("", String::concat);
    // System.out.println(concat);
    // assert ce.getID().equals(concat);
  }
}
