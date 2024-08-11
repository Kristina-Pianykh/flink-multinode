package com.huberlin.javacep.communication;

import com.huberlin.event.Event;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.ForwardingTable;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPEventSender implements SinkFunction<Tuple2<Integer, Event>> {
  private static final Logger LOG = LoggerFactory.getLogger(TCPEventSender.class);
  private final ForwardingTable fwd_table;
  private final HashMap<Integer, TCPAddressString> address_book;
  private final Map<TCPAddressString, PrintWriter> connections = new HashMap<>();
  private final int nodeid;

  public TCPEventSender(
      HashMap<Integer, TCPAddressString> address_book, ForwardingTable fwd_table, int nodeid) {
    this.nodeid = nodeid;
    this.fwd_table = fwd_table;
    this.address_book = address_book;
  }

  /** Called by flink to send events */
  @Override
  public void invoke(Tuple2<Integer, Event> tuple_of_source_node_id_and_event, Context ignored) {
    Integer source_node_id = tuple_of_source_node_id_and_event.f0;
    Event event = tuple_of_source_node_id_and_event.f1;

    fwd_table.print();
    for (Integer node_id : fwd_table.lookup(event.getEventType(), source_node_id)) {
      TCPAddressString dst = address_book.get(node_id);
      send_to(event.toString(), dst);
    }
  }

  private void send_to(String message, TCPAddressString target_ip_port) {
    try {
      // if the connection to a forwarding target was not established yet then establish it
      if (!connections.containsKey(target_ip_port)) {
        try {
          String host = target_ip_port.getHost();
          int port = target_ip_port.getPort();

          Socket client_socket = new Socket(host, port);
          client_socket.setTcpNoDelay(true);
          client_socket.setKeepAlive(true);
          PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
          writer.println("I am " + nodeid);
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
