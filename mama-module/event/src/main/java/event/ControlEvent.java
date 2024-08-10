package com.huberlin.event;

// package com.huberlin;

import java.io.Serializable;
import java.util.*;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlEvent extends Message implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  private static final Logger LOG = LoggerFactory.getLogger(Event.class);
  public final boolean control = true;
  public Optional<Long> driftTimestamp;
  public Optional<Long> shiftTimestamp;

  public ControlEvent(Optional<Long> driftTimestamp, Optional<Long> shiftTimestamp) {
    this.driftTimestamp = driftTimestamp;
    this.shiftTimestamp = shiftTimestamp;
  }

  public long getTimestamp() {
    return 0L;
  }

  public String toString() {
    StringBuilder eventString = new StringBuilder("control");
    String driftTimestampString =
        this.driftTimestamp.isPresent() ? formatTimestamp(this.driftTimestamp.get()) : "null";
    String shiftTimestampString =
        this.shiftTimestamp.isPresent() ? formatTimestamp(this.shiftTimestamp.get()) : "null";
    eventString.append(" | ").append(driftTimestampString);
    eventString.append(" | ").append(shiftTimestampString);
    return eventString.toString();
  }

  public static Optional<ControlEvent> parse(String message) {
    Optional<Long> driftTimestamp;
    Optional<Long> shiftTimestamp;

    String[] receivedParts = message.split("\\|");
    if (!receivedParts[0].trim().contains("control")) {
      LOG.error("Control message does not start with 'control'!");
      return Optional.empty();
    }

    ArrayList<String> attributeList = new ArrayList<>();
    for (int i = 1; i < receivedParts.length; i++) {
      attributeList.add(receivedParts[i].trim());
    }

    try {
      assert attributeList.size() >= 2;
    } catch (AssertionError e) {
      LOG.error("Control message does not contain enough attributes");
      return Optional.empty();
    }

    if (attributeList.get(0).equals("null")) driftTimestamp = Optional.empty();
    else driftTimestamp = Optional.of(parseTimestamp(attributeList.get(0)));

    if (attributeList.get(1).equals("null")) shiftTimestamp = Optional.empty();
    else shiftTimestamp = Optional.of(parseTimestamp(attributeList.get(1)));

    ControlEvent controlEvent = new ControlEvent(driftTimestamp, shiftTimestamp);
    return Optional.of(controlEvent);
  }
}
