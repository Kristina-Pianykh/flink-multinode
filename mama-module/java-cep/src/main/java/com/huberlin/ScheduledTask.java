package com.huberlin.javacep;

import com.huberlin.event.Event;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.ForwardingTable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTask implements Runnable {
  public int nodeId;
  public ForwardingTable fwdTable;
  public HashMap<Integer, TCPAddressString> addressBook;
  public Set<Event> eventBuffer;
  public String eventType;
  public HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSocketWriterMap;
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledTask.class);

  public ScheduledTask(
      Set<Event> eventBuffer,
      HashMap<Integer, TCPAddressString> addressBook,
      ForwardingTable fwdTable,
      int nodeId,
      String eventType) {
    this.eventBuffer = eventBuffer;
    this.addressBook = addressBook;
    this.fwdTable = fwdTable;
    this.nodeId = nodeId;
    this.eventType = eventType;

    LOG.info("initializing socket writer map");
    try {
      this.dstSocketWriterMap = initSocketWriterMap(this.eventType);
      printSocketWriterMap();
      LOG.info("Scheduled task created for node " + nodeId);
    } catch (Exception exc) {
      exc.printStackTrace();
    }
  }

  public void printSocketWriterMap() {
    for (Map.Entry<Integer, Tuple2<Socket, PrintWriter>> entry :
        this.dstSocketWriterMap.entrySet()) {
      System.out.println("Dest Node ID: " + entry.getKey());
      System.out.println("Socket: " + entry.getValue().f0);
      System.out.println("Writer: " + entry.getValue().f1);
    }
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
        LOG.error("Server is not running. Please start the server and try again.");
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
    System.out.println("=========================================");
    LOG.info("FLUSHING BUFFERED PARTITIONING INPUTS OF SIZE: {}", this.eventBuffer.size());
    LOG.info(this.eventBuffer.toString());
    System.out.println("=========================================");

    // try {
    //   assert (this.eventBuffer.stream()
    //       .allMatch(event -> event.getEventType().equals(this.eventType)));
    // } catch (AssertionError e) {
    //   LOG.error("Event type mismatch in the event buffer");
    //   e.printStackTrace();
    // }

    LOG.info("Number of socket-writers: {}", this.dstSocketWriterMap.size());
    for (Tuple2<Socket, PrintWriter> dstSocketWriter : this.dstSocketWriterMap.values()) {
      LOG.info("Writing to socket: {}", dstSocketWriter.f0);
      for (Event event : this.eventBuffer) {
        dstSocketWriter.f1.println(event.toString());
        dstSocketWriter.f1.checkError();
      }
      dstSocketWriter.f1.flush();
    }

    this.eventBuffer.clear();
    closeAllWriterSocket();
  }
}
