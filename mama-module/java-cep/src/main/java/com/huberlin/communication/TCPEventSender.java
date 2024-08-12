package com.huberlin.javacep.communication;

import com.huberlin.event.ControlEvent;
import com.huberlin.event.Event;
import com.huberlin.event.Message;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.ForwardingTable;
import com.huberlin.monitor.TimeUtils;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPEventSender implements SinkFunction<Tuple2<Integer, Message>> {
  private static final Logger LOG = LoggerFactory.getLogger(TCPEventSender.class);
  public AtomicReference<ForwardingTable> fwdTableRef;
  private final ForwardingTable updatedFwdTable;
  private final HashMap<Integer, TCPAddressString> addressBook;
  private final Map<TCPAddressString, PrintWriter> connections = new HashMap<>();
  private final int nodeId;
  private boolean transitionedState = false;
  private Long shiftTimestamp = 0L;

  public TCPEventSender(
      HashMap<Integer, TCPAddressString> addressBook,
      AtomicReference<ForwardingTable> fwdTableRef,
      ForwardingTable updatedFwdTable,
      int nodeId) {
    this.nodeId = nodeId;
    this.fwdTableRef = fwdTableRef;
    this.updatedFwdTable = updatedFwdTable;
    this.addressBook = addressBook;
  }

  /** Called by flink to send events */
  @Override
  public void invoke(Tuple2<Integer, Message> tuple_of_source_node_id_and_event, Context ignored) {
    Integer srcNodeId = tuple_of_source_node_id_and_event.f0;
    Message message = tuple_of_source_node_id_and_event.f1;

    boolean beforeShiftTime = TimeUtils.getCurrentTimeInMicroseconds() < this.shiftTimestamp;
    if ((this.shiftTimestamp > 0) && !beforeShiftTime && !this.transitionedState) {
      LOG.info("Updating forwarding table");
      this.transitionedState = true;
    }

    if (message instanceof ControlEvent) {
      ControlEvent controlEvent = (ControlEvent) message;
      LOG.info("Received control event: {}", controlEvent.toString());

      try {
        assert controlEvent.shiftTimestamp.isPresent();
      } catch (Exception e) {
        LOG.error(
            "shiftTimestamp in control event {} is not present: {}",
            controlEvent.toString(),
            e.getMessage());
        throw e;
      }
      this.shiftTimestamp = controlEvent.shiftTimestamp.get();
      LOG.info(
          "Shift timestamp set to: {} for control event {}",
          TimeUtils.format(this.shiftTimestamp),
          controlEvent.toString());

    } else {
      try {
        assert message instanceof Event;
      } catch (Exception e) {
        LOG.error(
            "Message {} is not instance of ControlEvent or Event: {}",
            message.toString(),
            e.getMessage());
        throw e;
      }

      Event event = (Event) message;
      ForwardingTable targetTable;
      SortedSet<Integer> dsts;
      LOG.info(
          "this.shiftTimestamp: {}; this.transitionedState: {}",
          this.shiftTimestamp,
          this.transitionedState);
      if (this.transitionedState) {
        targetTable = this.updatedFwdTable;
        dsts = targetTable.lookupUpdated(event.getEventType());
        LOG.info(
            "this.shiftTimestamp: {}; this.transitionedState: {}. Using updated forwarding table"
                + " for event {}. Destination nodes: {}",
            this.shiftTimestamp,
            this.transitionedState,
            event.toString(),
            dsts);
      } else {
        targetTable = fwdTableRef.get();
        dsts = targetTable.lookup(event.getEventType(), srcNodeId);
        LOG.info(
            "this.shiftTimestamp: {}; this.transitionedState: {}. Using original forwarding table"
                + " for event {} Destination nodes: {}",
            this.shiftTimestamp,
            this.transitionedState,
            event.toString(),
            dsts);
      }

      targetTable.print();

      for (Integer nodeId : dsts) {
        LOG.info("Forwarding event to node {}", nodeId);
        TCPAddressString dst = this.addressBook.get(nodeId);
        sendTo(event.toString(), dst);
      }
    }
  }

  private void sendTo(String message, TCPAddressString target_ip_port) {
    LOG.info("sendTo(): forwarding message: {} to {}", message, target_ip_port);
    try {
      // if the connection to a forwarding target was not established yet then establish it
      if (!connections.containsKey(target_ip_port)) {
        try {
          String host = target_ip_port.getHost();
          int port = target_ip_port.getPort();

          Socket client_socket = new Socket(host, port);
          client_socket.setTcpNoDelay(true);
          client_socket.setKeepAlive(true);
          // TODO: use json serialization for event objects?
          PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
          writer.println("I am " + nodeId);
          connections.put(target_ip_port, writer);
          LOG.info("Connection for forwarding events to " + target_ip_port + " established");
        } catch (Exception e) {
          LOG.error(
              "Failure to establish connection to "
                  + target_ip_port
                  + " for forwarding events. Error: "
                  + e);
          e.printStackTrace(System.err);
          System.exit(1);
        }
      }
      LOG.info("Established connection to {}. println message: {}", target_ip_port, message);
      connections.get(target_ip_port).println(message);
    } catch (Exception e) {
      LOG.error("Forwarding Error: " + e + " - Message:" + message + " to " + target_ip_port);
      e.printStackTrace(System.err);
    }
  }

  @Override
  public void finish() {
    for (PrintWriter conn : connections.values()) {
      conn.flush();
    }
  }
}
