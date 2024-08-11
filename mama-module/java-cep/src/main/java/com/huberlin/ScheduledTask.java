package com.huberlin.javacep;

import com.huberlin.event.Event;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.javacep.config.ForwardingTable;
import com.huberlin.sharedconfig.RateMonitoringInputs;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTask implements Runnable {
  public Set<Event> eventBuffer;
  public AtomicReference<ForwardingTable> fwdTableRef;
  public ForwardingTable updatedFwdTable;
  public RateMonitoringInputs rateMonitoringInputs;
  public int nodeId;
  public HashMap<Integer, TCPAddressString> addressBookTCP;
  public AtomicBoolean multiSinkQueryEnabled;
  public String eventType;
  public HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSocketWriterMap;
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledTask.class);

  public ScheduledTask(
      Set<Event> eventBuffer,
      AtomicReference<ForwardingTable> fwdTableRef,
      ForwardingTable updatedFwdTable,
      RateMonitoringInputs rateMonitoringInputs,
      int nodeId,
      HashMap<Integer, TCPAddressString> addressBookTCP,
      AtomicBoolean multiSinkQueryEnabled) {
    this.eventBuffer = eventBuffer;
    assert (this.eventBuffer != null);
    this.fwdTableRef = fwdTableRef;
    assert (this.fwdTableRef != null);
    this.updatedFwdTable = updatedFwdTable;
    assert (this.updatedFwdTable != null);
    this.rateMonitoringInputs = rateMonitoringInputs;
    assert (this.rateMonitoringInputs != null);
    this.nodeId = nodeId;
    assert (this.nodeId >= 0);
    this.addressBookTCP = addressBookTCP;
    assert (this.addressBookTCP != null);
    this.multiSinkQueryEnabled = multiSinkQueryEnabled;
    assert (this.multiSinkQueryEnabled != null);
    this.eventType = this.rateMonitoringInputs.partitioningInput;
    assert (this.eventType != null);
    LOG.info("initializing socket writer map");
    try {
      initSocketWriterMap(this.eventType);
      System.out.println("initialized socket writer map");
      printSocketWriterMap();
      LOG.info("Scheduled task created for node " + this.nodeId);
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
    SortedSet<Integer> dstNodeIds = this.updatedFwdTable.lookupUpdated(eventType);
    System.out.println("Destination Node IDs: " + dstNodeIds);
    // there must be only one dest ideally but the part input could be also used elsewhere??????
    // HashMap<Integer, Tuple2<Socket, PrintWriter>> dstSockets = new HashMap<>();
    this.dstSocketWriterMap = new HashMap<>();

    for (Integer dstNodeId : dstNodeIds) {
      try {
        TCPAddressString dst = this.addressBookTCP.get(dstNodeId);
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
          writer.println("I am " + this.nodeId);
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
    LOG.debug("Forwarding table before update: {}", this.fwdTableRef.get().toString());
    // this.config.forwarding.get().table = this.config.forwarding.get().updatedTable;
    this.fwdTableRef.set(this.updatedFwdTable);

    LOG.debug("multiSinkQueryEnabled before update: {}", this.multiSinkQueryEnabled.get());
    this.multiSinkQueryEnabled.set(false);
    LOG.debug("multiSinkQueryEnabled after update: {}", this.multiSinkQueryEnabled.get());
    // LOG.info(
    //     "Multi-Sink query {} disabled on the current node {}",
    //     this.rateMonitoringInputs.multiSinkQuery,
    //     this.nodeId);

    try {
      assert this.fwdTableRef.get().equals(this.updatedFwdTable);
    } catch (NullPointerException e) {
      LOG.error("Update of forwarding table resulted in null");
      e.printStackTrace();
    } catch (AssertionError e) {
      LOG.error(
          "Update of forwarding table failed\n. this.config.forwarding.table = {}\n"
              + "this.config.forwarding.updatedTable = {}",
          this.fwdTableRef.get().toString(),
          this.updatedFwdTable.toString());
      e.printStackTrace();
    }

    LOG.info("Forwarding table updated successfully");
    LOG.info("Forwarding table after update: {}", this.fwdTableRef.get().toString());
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
