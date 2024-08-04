package com.huberlin.javacep;

import com.huberlin.event.Event;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.NodeConfig;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTask implements Runnable {
  public NodeConfig config;
  public Set<Event> eventBuffer;
  public String eventType;
  public AtomicBoolean multiSinkQueryEnabled;
  public HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSocketWriterMap;
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledTask.class);

  public ScheduledTask(
      Set<Event> eventBuffer, NodeConfig config, AtomicBoolean multiSinkQueryEnabled) {
    this.eventBuffer = eventBuffer;
    this.config = config;
    this.eventType = this.config.rateMonitoringInputs.partitioningInput;
    this.multiSinkQueryEnabled = multiSinkQueryEnabled;
    LOG.info("initializing socket writer map");
    try {
      initSocketWriterMap(this.eventType);
      System.out.println("initialized socket writer map");
      printSocketWriterMap();
      LOG.info("Scheduled task created for node " + this.config.nodeId);
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

  public void initSocketWriterMap(String eventType) {
    SortedSet<Integer> dstNodeIds = this.config.forwarding.updatedTable.lookupUpdated(eventType);
    System.out.println("Destination Node IDs: " + dstNodeIds);
    // there must be only one dest ideally but the part input could be also used elsewhere??????
    // HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSockets = new HashMap<>();
    this.dstSocketWriterMap = new HashMap<>();

    for (Integer dstNodeId : dstNodeIds) {
      try {
        TCPAddressString dst = this.config.forwarding.addressBookTCP.get(dstNodeId);
        String host = dst.getHost();
        System.out.println("Host: " + host);
        int port = dst.getPort();
        System.out.println("Port: " + port);
        try {
          this.dstSocketWriterMap.put(dstNodeId, new Tuple2<>());
          Socket client_socket = new Socket(host, port);
          PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
          this.dstSocketWriterMap.get(dstNodeId).f0 = client_socket;
          this.dstSocketWriterMap.get(dstNodeId).f1 = writer;
          writer.println("I am " + this.config.nodeId);
          System.out.println("Connection for forwarding events to " + dst + " established");
        } catch (ConnectException e) {
          LOG.error("Server is not running. Please start the server and try again.");
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      } catch (Exception exc) {
        LOG.error("Error while getting host and port from address book");
        exc.printStackTrace();
      }
    }
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
    LOG.info("Updating forwarding table...");
    LOG.debug("Forwarding table before update: {}", this.config.forwarding.table.toString());
    this.config.forwarding.table = this.config.forwarding.updatedTable;

    LOG.debug("multiSinkQueryEnabled before update: {}", this.multiSinkQueryEnabled.get());
    this.multiSinkQueryEnabled.set(false);
    LOG.debug("multiSinkQueryEnabled after update: {}", this.multiSinkQueryEnabled.get());
    LOG.info(
        "Multi-Sink query {} disabled on the current node {}",
        this.config.rateMonitoringInputs.multiSinkQuery,
        this.config.nodeId);

    try {
      assert this.config.forwarding.table.equals(this.config.forwarding.updatedTable);
    } catch (NullPointerException e) {
      LOG.error("Update of forwarding table resulted in null");
      e.printStackTrace();
    } catch (AssertionError e) {
      LOG.error(
          "Update of forwarding table failed\n. this.config.forwarding.table = {}\n"
              + "this.config.forwarding.updatedTable = {}",
          this.config.forwarding.table.toString(),
          this.config.forwarding.updatedTable.toString());
      e.printStackTrace();
    }

    LOG.info("Forwarding table updated successfully");
    LOG.info("Forwarding table after update: {}", this.config.forwarding.table.toString());
    LOG.info(
        "===============FLUSHING BUFFERED PARTITIONING INPUTS OF SIZE:" + " {}===============",
        this.eventBuffer.size());
    LOG.info(this.eventBuffer.toString());

    try {
      assert (this.eventBuffer.stream()
          .allMatch(event -> event.getEventType().equals(this.eventType)));
    } catch (AssertionError e) {
      LOG.error("Event type mismatch in the event buffer");
      e.printStackTrace();
    }

    LOG.info("Number of socket-writers: {}", this.dstSocketWriterMap.size());
    for (Tuple2<Socket, PrintWriter> dstSocketWriter : this.dstSocketWriterMap.values()) {
      LOG.info("Writing to socket: {}", dstSocketWriter.f0);
      for (Event event : this.eventBuffer) {
        dstSocketWriter.f1.println(event.toString());
        dstSocketWriter.f1.checkError();
      }
    }

    this.eventBuffer.clear();
    LOG.info("Buffer cleared. Size of the buffer now: {}", this.eventBuffer.size());
    LOG.info("Closing all sockets and associated writers...");
    closeAllWriterSocket();
  }
}
