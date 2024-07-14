package com.huberlin.javacep.util;

import java.io.Serializable;

/**
 * A simple wrapper for strings. Subclass this to define 'Subtypes of String' to have more
 * expressive types for Variables.
 *
 * <p>You can also validate using the constructor, e.g. check that a TCP port is in the right range
 * or something.
 *
 * <p>Should be serializabe since the only data is the string, which is even immutable
 *
 * <p>Only direct subclasses are allowed to avoid unexpected behaviour with overriding equals,
 * hashcode.
 */
public abstract class StringWrapper implements Serializable {
  protected final String the_string;

  public StringWrapper(String the_string) {
    if (the_string == null) {
      throw new IllegalArgumentException(
          "Null is not a valid " + this.getClass().getCanonicalName() + ".");
    }
    this.the_string = the_string;
  }

  // methods to ensure the expected things happen
  @Override
  public final String toString() {
    return the_string;
  }

  @Override
  public final int hashCode() {
    return the_string.hashCode();
  }

  @Override
  public final boolean equals(Object other) {
    if (!(other.getClass()
        == this.getClass())) // Ensures subclasses only compare equal if they are the same subclass
    return false;
    return this.the_string.equals(other.toString()); // and they have the same string representation
  }
}
