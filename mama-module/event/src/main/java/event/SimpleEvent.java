package com.huberlin.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleEvent extends Event implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleEvent.class);
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  String eventID;
  public final long timestamp;

  public SimpleEvent(String eventID, long timestamp, String eventType, List<String> attributeList) {
    super(attributeList);
    if (attributeListPresent()) {
      try {
        assert (this.attributeList.size() >= 0);
        Boolean multiSinkQueryEnabled = Boolean.valueOf(this.attributeList.remove(0));
        assert (multiSinkQueryEnabled.getClass() == Boolean.class);
        this.multiSinkQueryEnabled = multiSinkQueryEnabled;
      } catch (AssertionError e) {
        LOG.error(
            "Function call new SimpleEvent({}, {}, {}, {}) failed. Assertions about attributeList"
                + " in SimpleEvent constructor failed with {}",
            eventID,
            timestamp,
            eventType,
            attributeList,
            e);
        throw e;
      }
    }
    this.is_simple = true;
    this.eventType = eventType;
    this.eventID = eventID;
    this.timestamp = timestamp;
    assert (eventType != null);
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
    eventString.append(" | ").append(this.multiSinkQueryEnabled);
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
