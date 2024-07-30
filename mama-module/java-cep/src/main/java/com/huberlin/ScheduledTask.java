package com.huberlin.javacep;

import com.huberlin.event.Event;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.ForwardingTable;
import com.huberlin.javacep.config.NodeAddress;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class ScheduledTask extends TimerTask {
  public int nodeId;
  public ForwardingTable fwdTable;
  public HashMap<Integer, TCPAddressString> addressBook;
  public Set<Event> eventBuffer;
  public String eventType;
  public HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSocketWriterMap;
  public Timer timer;

  public ScheduledTask(
      Set<Event> eventBuffer,
      Map<Integer, NodeAddress> addressBook,
      ForwardingTable fwdTable,
      int nodeId,
      String eventType,
      Timer timer) {
    this.eventBuffer = eventBuffer;
    this.nodeId = nodeId;
    this.fwdTable = fwdTable;
    this.eventType = eventType;
    this.addressBook = new HashMap<>();
    this.timer = timer;

    // check that address book contains entries for all node ids
    for (Integer node_id : fwdTable.get_all_node_ids())
      if (!addressBook.containsKey(node_id))
        throw new IllegalArgumentException(
            "The address book does not have an entry for the node ID " + node_id);
    // convert node ids to tcp address strings in address book
    for (Integer node_id : addressBook.keySet())
      this.addressBook.put(node_id, new TCPAddressString(addressBook.get(node_id).getEndpoint()));

    System.out.println("initializing socket writer map");
    this.dstSocketWriterMap = initSocketWriterMap(this.eventType);
    System.out.println("Scheduled task created for node " + nodeId);
  }

  public HashMap<Integer, Tuple2<Socket, PrintWriter>> initSocketWriterMap(String eventType) {
    SortedSet<Integer> dstNodeIds = fwdTable.lookupUpdated(eventType);
    // there must be only one dest ideally but the part input could be also used elsewhere??????
    HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSockets = new HashMap<>();

    for (Integer dstNodeId : dstNodeIds) {
      TCPAddressString dst = this.addressBook.get(dstNodeId);
      String host = dst.getHost();
      int port = dst.getPort();
      try {
        dstSockets.put(dstNodeId, new Tuple2<>());
        Socket client_socket = new Socket(host, port);
        dstSockets.get(dstNodeId).f0 = client_socket;
        dstSockets.get(dstNodeId).f1 = new PrintWriter(client_socket.getOutputStream(), true);
      } catch (ConnectException e) {
        System.out.println("Server is not running. Please start the server and try again.");
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return dstSocketWriterMap;
  }

  public void closeAllWriterSocket() {
    for (Tuple2<Socket, PrintWriter> dstSocketWriter : this.dstSocketWriterMap.values()) {
      try {
        dstSocketWriter.f1.close();
        dstSocketWriter.f0.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void run() {
    System.out.println("FLUSHING BUFFERED PARTITIONING INPUTS OF SIZE: " + this.eventBuffer.size());
    assert (this.eventBuffer.stream()
        .allMatch(event -> event.getEventType().equals(this.eventType)));

    for (Tuple2<Socket, PrintWriter> dstSocketWriter : this.dstSocketWriterMap.values()) {
      for (Event event : this.eventBuffer) {
        dstSocketWriter.f1.println(event.toString());
      }
      dstSocketWriter.f1.flush();
    }

    this.eventBuffer.clear();
    closeAllWriterSocket();
    this.timer.cancel();
    this.timer.purge();
  }
}
