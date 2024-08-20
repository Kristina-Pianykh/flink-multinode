// package monitor;
package com.huberlin.monitor;

import com.huberlin.event.Event;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingEventBuffer extends ArrayBlockingQueue<Event> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockingEventBuffer.class);

  public BlockingEventBuffer(int capacity) {
    super(capacity);
  }

  public void resize() {
    int newCapacity = size() * 2;
    int numElements = this.size();
    ArrayBlockingQueue<Event> newQueue = new ArrayBlockingQueue<>(newCapacity);
    this.drainTo(newQueue);
    assert (numElements == newQueue.size());
  }

  public void clearOutdatedEvents(long cutoffTimestamp) {
    // System.out.println("\nBuffer size before dropping old events: " + this.size());
    // System.out.println(this.toString());
    // System.out.println(
    //     "Dropping events with timestamp <= " + FormatTimestamp.format(cutoffTimestamp));
    // System.out.println("Buffer size before dropping old events: " + this.size());
    // this.removeIf(e -> e.getTimestamp() <= cutoffTimestamp);
    for (Event e : this) {
      LOG.info(
          "e.getTimestamp() ("
              + e.getTimestamp()
              + ") <= cutoffTimestamp ("
              + cutoffTimestamp
              + ") = "
              + (e.getTimestamp() <= cutoffTimestamp));
      // System.out.println(
      //     "e.getTimestamp() ("
      //         + e.getTimestamp()
      //         + ") <= cutoffTimestamp ("
      //         + cutoffTimestamp
      //         + ") = "
      //         + (e.getTimestamp() <= cutoffTimestamp));
      if (e.getTimestamp() <= cutoffTimestamp) {
        this.remove(e);
      }
    }
    // System.out.println("Buffer size after dropping old events: " + this.size() + "\n");
  }

  public String toString() {
    int indent = 3;
    StringBuilder sb = new StringBuilder();
    sb.append("BlockingEventBuffer{");
    sb.append("\n");
    for (Event e : this) {
      for (int i = 0; i < indent; i++) {
        sb.append(" ");
      }
      sb.append(e);
      sb.append("\n");
    }
    sb.append("}");
    return sb.toString();
  }
}
