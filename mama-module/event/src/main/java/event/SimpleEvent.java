package com.huberlin.event;

// package com.huberlin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SimpleEvent extends Event implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  String eventID;
  public final long timestamp;
  public List<String> attributeList;

  //    public String eventType;
  public SimpleEvent(String eventID, long timestamp, String eventType, List<String> attributeList) {
    this.is_simple = true;
    this.eventType = eventType;
    this.eventID = eventID;
    this.timestamp = timestamp;
    assert (eventType != null);
    this.attributeList = attributeList;
  }

  @Override
  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public long getHighestTimestamp() {
    return this.getTimestamp();
  }

  @Override
  public long getLowestTimestamp() {
    return this.getTimestamp();
  }

  /**
   * @param event_type a primitive event type
   * @return ID of this event, or null if type isn't this event's type
   */
  @Override
  public String getEventIdOf(String event_type) {
    if (event_type.equals(this.eventType)) return this.eventID;
    else return null;
  }

  public void setEventID(String new_eventID) {
    this.eventID = new_eventID;
  }

  // REAL WORLD FUNCTION
  @Override
  public SimpleEvent getEventOfType(String event_type) {
    if (event_type.equals(this.eventType)) return this;
    else return null;
  }

  /**
   * @param event_type a primitive event type
   * @return the timestamp, or null if a type other than this primitive type is given
   */
  @Override
  public Long getTimestampOf(String event_type) {
    if (event_type.equals(this.eventType)) return this.timestamp;
    else return null;
  }

  public String toString() {
    StringBuilder eventString = new StringBuilder("simple");
    eventString.append(" | ").append(this.eventID);
    eventString.append(" | ").append(formatTimestamp(this.timestamp));
    eventString.append(" | ").append(this.eventType);
    for (String attributeValue : this.attributeList)
      eventString.append(" | ").append(attributeValue);
    return eventString.toString();
  }

  public String getID() {
    return this.eventID;
  }

  @Override
  public ArrayList<SimpleEvent> getContainedSimpleEvents() {
    ArrayList<SimpleEvent> result = new ArrayList<>(1);
    result.add(this);
    return result;
  }

  public List<String> getAttributeList() {
    return this.attributeList;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SimpleEvent)) {
      return false;
    } else if (other == this) return true;
    else return this.eventID.equals(((SimpleEvent) other).eventID);
  }

  @Override
  public int hashCode() {
    return this.eventID.hashCode();
  }
}
