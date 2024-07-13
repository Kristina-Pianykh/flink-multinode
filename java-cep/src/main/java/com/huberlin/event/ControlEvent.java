package com.huberlin.event;

import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ControlEvent implements Serializable {
  private static final long serialVersionUID = 1L; // Add a serialVersionUID for Serializable class

  private static final Logger log = LoggerFactory.getLogger(Event.class);

  // -------------------- Getter/Setter --------------------

  // set-methods should not be provided (effectively immutable object)
  public abstract long getTimestamp();

  /**
   * Serialize to string
   *
   * @return the unique string representation of this event
   */
  public abstract String toString();

  // -------------------- Helper functions, static, stateless  --------------------
  // --- Static methods for serialization (to string) and deserialization (from string) ---

  /**
   * Convert a string representation of an event to Event form
   *
   * @param received A event's unique string representation.
   * @return The event
   */
}
