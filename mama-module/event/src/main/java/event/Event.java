package com.huberlin.event;

// package com.huberlin;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Event extends Message {

  private static final Logger LOG = LoggerFactory.getLogger(Event.class);
  boolean is_simple;
  public String eventType;
  public boolean multiSinkQueryEnabled = true;
  public List<String> attributeList;

  public Event(List<String> attributeList) {
    this.attributeList = attributeList;
  }

  // -------------------- Getter/Setter --------------------

  // set-methods should not be provided (effectively immutable object)
  public boolean isSimple() {
    return this.is_simple;
  }

  public String getEventType() {
    return this.eventType;
  }

  public abstract ArrayList<SimpleEvent> getContainedSimpleEvents();

  public void setMultiSinkQueryEnabled(boolean multiSinkQueryEnabled) {
    this.multiSinkQueryEnabled = multiSinkQueryEnabled;
  }

  public boolean attributeListPresent() {
    if (attributeList == null) return false;
    if (attributeList.isEmpty()) return false;
    return true;
  }

  public boolean isFlushed() {
    if (!attributeListPresent()) return false;
    else {
      for (String attr : this.attributeList) {
        if (attr.contains("flushed")) return true;
      }
      return false;
    }
  }

  public void addAttribute(String attr) {
    if (this.attributeList == null) {
      LOG.debug("attributeList = null");
      this.attributeList = new ArrayList<>();
      LOG.debug("attributeList initialized");
      this.attributeList.add(attr);
      return;
    }
    this.attributeList.add(attr);
    assert this.attributeList.contains(attr);
  }

  /**
   * Get the timestamp used for watermarking
   *
   * @return the timestamp
   */
  public abstract long getHighestTimestamp();

  public abstract long getLowestTimestamp();

  /*+
   * Get id of constituent simple event, by type. Must be fast - for flinkcep conditions.
   */
  public abstract String getEventIdOf(String event_type);

  public abstract Long getTimestampOf(String event_type);

  public abstract long getTimestamp();

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
    ArrayList<String> attributeList = new ArrayList<>();
    if (receivedParts[0].trim().equals("simple")) {
      // simple event: "simple" | eventID | timestamp | eventType | multiSinkQueryEnabled | optional
      // attributes
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

      return new SimpleEvent(
          receivedParts[1].trim(), // eventID
          parseTimestamp(receivedParts[2].trim()), // timestamp
          receivedParts[3].trim(), // eventType
          // multiSinkQueryEnabled,
          attributeList); // attributeList
      //
    } else if (receivedParts[0].trim().equals("complex")) {
      ComplexEvent ce = null;
      // complex event: "complex" | eventID | timestamp [can be creationTime] | eventType |
      // numberOfEvents |
      // (individual Event);(individual Event)[;...] | optional attributes
      String eventID = receivedParts[1].trim();
      long timestamp = parseTimestamp(receivedParts[2].trim());
      String eventType = receivedParts[3].trim();
      int numberOfEvents =
          Integer.parseInt(
              receivedParts[4].trim()); // FIXME: Currently, we don't use this information at all.
      ArrayList<SimpleEvent> eventList = parse_eventlist(receivedParts[5].trim());

      try {
        assert (eventID != null);
        assert (timestamp > 0);
        assert (eventType != null);
        assert (numberOfEvents == eventList.size());
        assert (numberOfEvents > 0);
        assert (eventList.size() > 0);
      } catch (AssertionError e) {
        LOG.error(
            "Failed to parse a complex event: {}. receivedParts: {}, eventID: {}, timestamp: {},"
                + " eventType: {}, numberOfEvents: {}, eventList: {}. Error: {}",
            received,
            receivedParts,
            eventID,
            timestamp,
            eventType,
            numberOfEvents,
            eventList,
            e.getMessage());
        System.exit(1);
      }

      ce = new ComplexEvent(eventID, timestamp, eventType, eventList, attributeList);
      try {
        assert ce != null;
      } catch (AssertionError err) {
        LOG.error(
            "Failed to create a complex event from message {}. Error: {}",
            received,
            err.getMessage());
        System.exit(1);
      }
      return ce;
    } else {
      // incomprehensible message
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
      String[] eventParts = event.split(","); // timestamp, id, type, attributes

      List<String> attributeList = new ArrayList<>();
      for (int i = 3; i < eventParts.length; i++) {
        attributeList.add(eventParts[i].trim());
      }

      if (eventParts.length > 3) {
        assert (Boolean.valueOf(eventParts[3].trim()) instanceof Boolean);
      }

      // one Event: (timestamp_hhmmssms , eventID , eventType, multiSinkQueryEnabled)
      events.add(
          new SimpleEvent(
              eventParts[1].trim(),
              parseTimestamp(eventParts[0].trim()),
              eventParts[2].trim(),
              // Boolean.valueOf(eventParts[3].trim()),
              attributeList));
    }
    return events;
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
