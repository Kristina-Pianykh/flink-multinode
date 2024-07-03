package monitor.event;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public abstract class Event extends ControlEvent {

  // private static final Logger log = LoggerFactory.getLogger(Event.class);
  boolean is_simple;
  public String eventType;

  // -------------------- Getter/Setter --------------------

  // set-methods should not be provided (effectively immutable object)
  public boolean isSimple() {
    return this.is_simple;
  }

  public String getEventType() {
    return this.eventType;
  }

  public abstract ArrayList<SimpleEvent> getContainedSimpleEvents();

  /**
   * Get the timestamp used for watermarking
   *
   * @return the timestamp
   */
  public abstract long getTimestamp();

  public abstract long getHighestTimestamp();

  public abstract long getLowestTimestamp();

  /*+
   * Get id of constituent simple event, by type. Must be fast - for flinkcep conditions.
   */
  public abstract String getEventIdOf(String event_type);

  public abstract Long getTimestampOf(String event_type);

  public abstract String getID();

  // REALWORLD EXPS
  public abstract SimpleEvent getEventOfType(String event_type);

  // -------------------- Helper functions, static, stateless  --------------------
  // --- Static methods for serialization (to string) and deserialization (from string) ---

  /**
   * Convert a string representation of an event to Event form
   *
   * @param received A event's unique string representation.
   * @return The event
   */
  public static Event parse(String received) {
    String[] receivedParts = received.split("\\|");
    if (receivedParts[0].trim().equals("simple")) {
      ArrayList<String> attributeList = new ArrayList<>();
      if (receivedParts.length > 4)
        attributeList.addAll(Arrays.asList(receivedParts).subList(4, receivedParts.length));

      for (int i = attributeList.size() - 1; i >= 0; i--) {
        String attribute = attributeList.get(i);
        String cleanedAttribute = attribute.trim();

        // Check if the cleaned string is empty (only whitespace)
        if (cleanedAttribute.isEmpty()) {
          // Remove the element from the list
          attributeList.remove(i);
        } else {
          // Update the original element with the cleaned value
          attributeList.set(i, cleanedAttribute);
        }
      }
      // simple event: "simple" | eventID | timestamp | eventType [| possible attributes according
      // to specific event class: define new parse and constructor]
      return new SimpleEvent(
          receivedParts[1].trim(), // eventID
          parseTimestamp(receivedParts[2].trim()), // timestamp
          receivedParts[3].trim(), // eventType
          attributeList); // attributeList
    } else if (receivedParts[0].trim().equals("complex")) {
      // complex event: "complex" | timestamp [can be creationTime] | eventType | numberOfEvents |
      // (individual Event);(individual Event)[;...]
      long timestamp = parseTimestamp(receivedParts[1].trim());
      String eventType = receivedParts[2].trim();
      int numberOfEvents =
          Integer.parseInt(
              receivedParts[3].trim()); // FIXME: Currently, we don't use this information at all.
      ArrayList<SimpleEvent> eventList = parse_eventlist(receivedParts[4].trim());
      return new ComplexEvent(timestamp, eventType, eventList);
    } else {
      // incomprehensible message
      // message = gpt4.interpretWhatThismeansAndConvertItToMyStandardFormat(message)
      throw new IllegalArgumentException(
          "Received message has wrong type: " + receivedParts[0].trim());
    }
  }

  /**
   * Parse an event list from the serialization format
   *
   * @param event_list transfer encoded primitive-event-list string
   * @return event list as java List of Simple Events
   */
  static ArrayList<SimpleEvent> parse_eventlist(String event_list) {
    String[] seperateEvents = event_list.split(";");
    int numberOfEvents = seperateEvents.length;
    ArrayList<SimpleEvent> events = new ArrayList<>(numberOfEvents);

    for (String event : seperateEvents) {
      event = event.trim(); // remove whitespace
      event = event.substring(1, event.length() - 1); // remove parentheses
      String[] eventParts = event.split(","); // timestamp, id, type

      List<String> attributeList = new ArrayList<>();
      for (int i = 3; i < eventParts.length; i++) {
        attributeList.add(eventParts[i].trim());
      }

      // one Event: (timestamp_hhmmssms , eventID , eventType)
      events.add(
          new SimpleEvent(
              eventParts[1].trim(),
              parseTimestamp(eventParts[0].trim()),
              eventParts[2].trim(),
              attributeList));
    }
    return events;
  }

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

  /**
   * Convert timestamp from long to string (serialized) form.
   *
   * @param timestamp numerical timestamp
   * @return timestamp in human-readable text form
   */
  public static String formatTimestamp(long timestamp) {
    LocalTime ts = LocalTime.ofNanoOfDay(timestamp * 1000L);
    return TIMESTAMP_FORMATTER.format(ts);
  }

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

  /**
   * Returns a HashSet containing the simple event types in the given complex event type.
   *
   * @param complex_event_type a term such as AND(A,SEQ(A,C))
   * @return a HashSet containing the simple events in the given complex event.
   */
  public static HashSet<String> getPrimitiveTypes(String complex_event_type)
      throws IllegalArgumentException {
    return getPrimitiveTypes(complex_event_type, new HashSet<>());
  }

  private static HashSet<String> getPrimitiveTypes(String term, HashSet<String> acc)
      throws IllegalArgumentException {
    final List<String> OPERATORS = Arrays.asList("AND", "SEQ");
    term = term.trim();

    if (term.length() == 1) {
      acc.add(term);
      return acc;
    }
    for (String operator : OPERATORS) {
      if (term.startsWith(operator + "(") && term.endsWith(")")) {
        List<String> args = new ArrayList<>();
        int paren_depth = 0;
        StringBuilder buf = new StringBuilder();
        for (int i = operator.length() + 1; i < term.length() - 1; i++) {
          char c = term.charAt(i);
          if (paren_depth == 0 && c == ',') {
            args.add(buf.toString());
            buf.delete(0, buf.length());
          } else buf.append(c);
          if (c == ')') paren_depth--;
          else if (c == '(') paren_depth++;

          if (paren_depth < 0)
            throw new IllegalArgumentException("Invalid complex event expression: " + term);
        }
        if (paren_depth > 0)
          throw new IllegalArgumentException("Invalid complex event expression: " + term);
        args.add(buf.toString());

        for (String arg : args) getPrimitiveTypes(arg, acc);
        return acc;
      }
    }
    throw new IllegalArgumentException(
        "Invalid complex event expression: "
            + term); // TODO: exact validation with context free grammar
  }
}
