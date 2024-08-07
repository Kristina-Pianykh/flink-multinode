package com.huberlin.event;

// package com.huberlin;

import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComplexEvent extends Event implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  private static final Logger LOG = LoggerFactory.getLogger(ComplexEvent.class);
  final ArrayList<SimpleEvent> eventList;

  String eventID;

  // The following fields are redundant (derived from the data above)
  // Since events are not mutated once created this cannot become inconsistent
  // All data access from flinkCEP iterative conditions should avoid computations, because it's
  // liable to be massively repeated, so add a field here andcompute it in the constructor.
  public final HashMap<String, Long> eventTypeToTimestamp;
  private final HashMap<String, String> eventTypeToEventID;
  private final long creation_timestamp;
  private final long highestTimestamp;
  private final long lowestTimestamp;

  public ComplexEvent(
      long creationTimestamp,
      String eventType,
      ArrayList<SimpleEvent> eventList,
      List<String> attributeList) {
    super(attributeList);
    this.is_simple = false;
    this.creation_timestamp = creationTimestamp;
    this.eventType = eventType; // and[A_B] | and[A_B_seq[C_D]]  bzw nur  and[A_B_seq[C_D]]
    this.eventList = eventList;

    // fill out the redundant fields (for performance optimization only)
    eventTypeToTimestamp = new HashMap<>();
    eventTypeToEventID = new HashMap<>();
    String event_ID = "";
    long highest_timestamp = Long.MIN_VALUE;
    long lowest_timestamp = Long.MAX_VALUE;
    for (SimpleEvent e : eventList) {
      highest_timestamp = Math.max(highest_timestamp, e.timestamp);
      lowest_timestamp = Math.min(lowest_timestamp, e.timestamp);
      eventTypeToTimestamp.put(e.eventType, e.timestamp);
      eventTypeToEventID.put(e.eventType, e.eventID);
      event_ID = event_ID + e.eventID;
    }

    this.highestTimestamp = highest_timestamp;
    this.lowestTimestamp = lowest_timestamp;
    this.eventID = event_ID;
    this.multiSinkQueryEnabled = multiSinkQueryEnabledForAllSimpleEvents(this.eventList);
  }

  public int getNumberOfEvents() {
    return this.eventList.size();
  }

  private static boolean multiSinkQueryEnabledForAllSimpleEvents(ArrayList<SimpleEvent> eventList) {
    return eventList.stream().allMatch(e -> e.multiSinkQueryEnabled);
  }

  public String getID() {
    return this.eventID;
  }

  public String getEventIdOf(String event_type) {
    return eventTypeToEventID.get(event_type);
  }

  // REAL WORLD SUPPORT
  @Override
  public SimpleEvent getEventOfType(String event_type) {
    for (SimpleEvent event : this.eventList) {
      if (event.eventType.equals(event_type)) {
        return event;
      }
    }
    return null;
  }

  /**
   * Return timestamp of given constituent primitive event, by type.
   *
   * @param event_type Primitive type
   * @return Timestamp
   */
  @Override
  public Long getTimestampOf(String event_type) {
    return eventTypeToTimestamp.get(event_type);
  }

  @Override
  public ArrayList<SimpleEvent> getContainedSimpleEvents() {
    return this.eventList;
  }

  /**
   * The creation timestamp of the CE. This is not used in matching. Only for latency computation.
   * It's not really a timestamp like the ones on the primitive events, it's the creation time.
   * TODO: rename?
   *
   * @return the timestamp in microseconds
   */
  @Override
  public long getTimestamp() {
    return this.creation_timestamp;
  }

  @Override
  public long getLowestTimestamp() {
    return this
        .lowestTimestamp; // we precompute this because this method needs to be accessed many times
    // per object (it's used in the flinkCEP iterative condition)
  }

  @Override
  public long getHighestTimestamp() {
    return this.highestTimestamp;
  }

  public double getLatencyMs() {
    double creationTime = (double) this.getTimestamp();
    double newestEventTs = (double) getHighestTimestamp();
    return (creationTime - newestEventTs) / 1000;
    // detection_latency_writer.write_record(latency);
  }

  public String toString() {
    StringBuilder eventString = new StringBuilder(this.isSimple() ? "simple" : "complex");
    eventString.append(" | ").append(formatTimestamp(this.creation_timestamp));
    eventString.append(" | ").append(this.eventType);
    eventString.append(" | ").append(this.getNumberOfEvents());
    eventString.append(" | ");

    for (int i = 0; i < eventList.size(); i++) {
      SimpleEvent e = this.eventList.get(i);
      eventString
          .append("(")
          .append(formatTimestamp(e.timestamp))
          .append(", ")
          .append(e.eventID)
          .append(", ")
          .append(e.eventType)
          .append(", ")
          .append(e.multiSinkQueryEnabled);
      for (String attributeValue : e.attributeList) {
        eventString.append(", ").append(attributeValue);
      }
      eventString.append(")");
      if (i < this.getNumberOfEvents() - 1) // no ";" after last event in the list
      eventString.append(" ;");
    }

    eventString.append(" | ").append(this.multiSinkQueryEnabled);

    if (this.attributeListPresent()) {
      for (String attributeValue : this.attributeList)
        eventString.append(" | ").append(attributeValue);
    }

    return eventString.toString();
  }
}
