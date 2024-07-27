package com.huberlin.event;

// package com.huberlin;

import java.io.Serializable;
import java.util.*;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlEvent extends Message implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class
  private static final Logger log = LoggerFactory.getLogger(Event.class);
  public final boolean control = true;
  public Long driftTimestamp = null;
  public Long shiftTimestamp = null;

  public ControlEvent(Long driftTimestamp, Long shiftTimestamp) {
    this.driftTimestamp = driftTimestamp;
    this.shiftTimestamp = shiftTimestamp;
  }

  public long getTimestamp() {
    return 0L;
  }

  public String toString() {
    StringBuilder eventString = new StringBuilder("control");
    String driftTimestampString =
        this.driftTimestamp == null ? "null" : formatTimestamp(this.driftTimestamp);
    String shiftTimestampString =
        this.shiftTimestamp == null ? "null" : formatTimestamp(this.shiftTimestamp);
    eventString.append(" | ").append(driftTimestampString);
    eventString.append(" | ").append(shiftTimestampString);
    return eventString.toString();
  }

  public static ControlEvent parse(String message) {
    Long driftTimestamp;
    Long shiftTimestamp;

    String[] receivedParts = message.split("\\|");
    if (!receivedParts[0].trim().contains("control")) {
      System.out.println("Control message does not start with 'control'!");
      return null;
    }

    ArrayList<String> attributeList = new ArrayList<>();
    for (int i = 1; i < receivedParts.length; i++) {
      attributeList.add(receivedParts[i].trim());
    }

    assert attributeList.size() >= 2;

    if (attributeList.get(0).equals("null")) {
      driftTimestamp = null;
    } else {
      driftTimestamp = parseTimestamp(attributeList.get(0));
    }

    if (attributeList.get(1).equals("null")) {
      shiftTimestamp = null;
    } else {
      shiftTimestamp = parseTimestamp(attributeList.get(1));
    }

    ControlEvent controlEvent = new ControlEvent(driftTimestamp, shiftTimestamp);
    return controlEvent;
  }
}
