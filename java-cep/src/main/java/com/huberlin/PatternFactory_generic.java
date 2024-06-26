package com.huberlin;

import com.google.common.collect.Collections2;
import com.huberlin.config.NodeConfig;
import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ThroughputLogger extends Thread {
  private final AtomicInteger counter;
  private final String file_path;

  public ThroughputLogger(AtomicInteger counter, String file_path) {
    this.counter = counter;
    this.file_path = file_path;
  }

  @Override
  public void run() {
    try (FileWriter writer = new FileWriter(file_path, true)) {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(10000);
          int count = counter.getAndSet(0);
          String log_line = count + "\n";
          writer.write(log_line);
          writer.flush();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (IOException e) {
          e.printStackTrace();
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

public class PatternFactory_generic {
  private static final Logger log = LoggerFactory.getLogger(PatternFactory_generic.class);
  private static AtomicInteger timestamp_counter = new AtomicInteger(0);
  private static long queryCounter = 0;

  public static List<DataStream<Event>> processQueries(
      List<NodeConfig.Processing> allQueries, DataStream<Event> inputStream) {
    List<DataStream<Event>> matchingStreams = new ArrayList<>();
    for (NodeConfig.Processing q : allQueries) {
      try {
        matchingStreams.add(processQuery(q, inputStream));
        queryCounter++; // Query counter for patterns
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return matchingStreams;
  }

  public static DataStream<Event> processQuery(
      NodeConfig.Processing query, DataStream<Event> inputStream) throws Exception {
    if (query.subqueries.isEmpty()) {
      throw new Exception("No subqueries defined");
    }

    DataStream<Event> output = processSubquery(0, query, inputStream);

    for (int i = 1; i < query.subqueries.size(); i++) {
      output = processSubquery(i, query, output);
    }

    return output;
  }

  public static DataStream<Event> processSubquery(
      final int num_subquery, NodeConfig.Processing queryInfo, DataStream<Event> inputStream) {
    List<String> inputs = queryInfo.inputs.get(num_subquery);

    // Get every permutation of inputs (order they could arrive in)
    Collection<List<String>> inputPermutations = Collections2.permutations(inputs);
    List<DataStream<Event>> streams = new ArrayList<>(2);

    // Generate a pattern for every possible input order
    for (List<String> inputPerm : inputPermutations)
      streams.add(
          generateStream(inputStream, inputPerm.get(0), inputPerm.get(1), queryInfo, num_subquery));

    DataStream<Event> output = inputStream;
    for (DataStream<Event> s : streams) output = output.union(s);

    return output;
  }

  public static DataStream<Event> generateStream(
      DataStream<Event> input,
      String firstEventType,
      String secondEventType,
      NodeConfig.Processing q,
      final int num_subquery) {
    final long TIME_WINDOW_SIZE_US = q.timeWindowSize * 1_000_000;
    List<List<String>> sequence_constraints = q.sequenceConstraints.get(num_subquery);
    List<String> idConstraints = q.idConstraints.get(num_subquery);
    double selectivity = q.selectivities.get(num_subquery);

    final String patternBaseName =
        "pattern_q" + queryCounter + "_sq" + num_subquery + "_" + firstEventType;
    final String firstPatternName = patternBaseName + "_0";
    final String secondPatternName = patternBaseName + "_1";

    Pattern<Event, Event> p =
        Pattern.<Event>begin(firstPatternName)
            .where(checkEventType(firstEventType)) // get first event type
            .followedByAny(secondPatternName)
            .where(checkEventType(secondEventType)) // get second event type
            .where(
                new IterativeCondition<Event>() { // Check timestamps, sequence and id constraints
                  long seed = 12345L;
                  final Random rand =
                      new Random(seed); // tmp: seed random stream for reproducibility

                  @Override
                  public boolean filter(Event new_event, Context<Event> context) throws Exception {
                    Thread.sleep(1);

                    if (new_event.getEventType().equals(secondEventType)) {
                      Iterable<Event> events = context.getEventsForPattern(firstPatternName);
                      Event old_event = null;
                      for (Event e : events) {
                        old_event = e;
                      }

                      // Check timestamp
                      if (Math.abs(old_event.getHighestTimestamp() - new_event.getLowestTimestamp())
                              > TIME_WINDOW_SIZE_US
                          || Math.abs(
                                  new_event.getHighestTimestamp() - old_event.getLowestTimestamp())
                              > TIME_WINDOW_SIZE_US) return false;

                      // Check selectivity
                      if (rand.nextDouble() > selectivity) return false;

                      // Check id constraint
                      for (String idConstraint : idConstraints) {
                        if (!old_event
                            .getEventIdOf(idConstraint)
                            .equals(new_event.getEventIdOf(idConstraint))) return false;
                      }

                      // Check sequence constraint (first > last or first < last)
                      // No sequence constraints = AND
                      for (List<String> sequence_constraint : sequence_constraints) {
                        String first_eventtype = sequence_constraint.get(0);
                        String second_eventtype = sequence_constraint.get(1);

                        // Sequence constraint check (for both directions)
                        if (old_event.getTimestampOf(first_eventtype) != null
                            && new_event.getTimestampOf(second_eventtype) != null
                            && old_event.getTimestampOf(first_eventtype)
                                >= new_event.getTimestampOf(second_eventtype)) {
                          return false;
                        }

                        if (new_event.getTimestampOf(first_eventtype) != null
                            && old_event.getTimestampOf(second_eventtype) != null
                            && new_event.getTimestampOf(first_eventtype)
                                >= old_event.getTimestampOf(second_eventtype)) {
                          return false;
                        }
                      }
                      return true; // Everything done
                    } else {
                      return false; // Not even the right type
                    }
                  }
                })
            .within(Time.milliseconds(TIME_WINDOW_SIZE_US));

    PatternStream<Event> matchStream = CEP.pattern(input, p);
    DataStream<Event> outputStream =
        matchStream.select(
            new PatternSelectFunction<Event, Event>() {

              @Override
              public Event select(Map<String, List<Event>> match) {
                Set<String> addedEvents = new HashSet<>();
                ArrayList<SimpleEvent> newEventList = new ArrayList<>();

                for (int i = 0; i <= 1; i++) { // For first (_0) and second (_1) pattern
                  for (Event evt : match.get(patternBaseName + "_" + i)) {
                    for (SimpleEvent contained : evt.getContainedSimpleEvents()) {
                      if (q.output_selection.contains(contained.getEventType())) {
                        boolean it_was_new =
                            addedEvents.add(
                                contained
                                    .getID()); // 'add new id to event in case of outputselection'
                        if (it_was_new) newEventList.add(contained);
                      }
                    }
                  }
                }

                long creation_time =
                    (LocalTime.now().toNanoOfDay()
                        / 1000L); // FIXME: What if it is almost midnight? Then the monsters come
                // out!
                ComplexEvent new_complex_event =
                    new ComplexEvent(creation_time, q.subqueries.get(num_subquery), newEventList);
                System.out.println(new_complex_event);
                return (Event) new_complex_event;
              }
            });

    return outputStream;
  }

  public static SimpleCondition<Event> checkEventType(String eventType) {
    return new SimpleCondition<Event>() {
      String latest_eventID = "";

      @Override
      public boolean filter(Event e) {
        String simp_eventID = e.getID();

        if (!simp_eventID.equals(latest_eventID)) {
          latest_eventID = simp_eventID;
          timestamp_counter.incrementAndGet();
        }

        return eventType.equals(e.getEventType());
      }
    };
  }
}
