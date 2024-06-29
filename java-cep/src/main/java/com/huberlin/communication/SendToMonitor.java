package com.huberlin.communication;

import com.huberlin.event.Event;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendToMonitor implements SinkFunction<Event> {
  private static final Logger log = LoggerFactory.getLogger(TCPEventSender.class);
  final int nodeId;
  final boolean isMultiSinkNode;
  private PrintWriter writer = null;

  private final int port;

  public SendToMonitor(int nodeId, int nodePort, boolean isMiltiSinkNode) {
    this.nodeId = nodeId;
    this.port = nodePort + 20;
    this.isMultiSinkNode = isMiltiSinkNode;
    System.out.println("SendToMonitor created with port " + port);
  }

  public Socket openSocket(int port, int attempt) {
    Socket socket = null;
    if (attempt > 5) {
      System.out.println("Could not open socket on port " + port);
      return null;
    }
    try {
      socket = new Socket("localhost", port);
      this.writer = new PrintWriter(socket.getOutputStream(), true);

      System.out.println("Socket opened on port " + port);
    } catch (IOException e) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      System.out.println("Could not open socket on port " + port);
      System.out.println("Trying again in 1 second...");
      socket = openSocket(port, attempt + 1);
    }
    return socket;
  }

  /** Called by flink to send events */
  @Override
  public void invoke(Event event, Context ignored) {
    if (!isMultiSinkNode) {
      return;
    }
    Socket socket = openSocket(port, 1);
    if (socket == null) {
      return;
    }
    System.out.println("SendToMonitor: " + event.toString());
    this.writer.println(event.toString());
    // out.writeObject(event.toString());
    // out.flush(); // Ensure data is sent out immediately
    // } finally {
    // try {
    //   if (out != null) {
    //     out.close();
    //   }
    //   if (socket != null) {
    //     socket.close();
    //   }
    // } catch (IOException e) {
    //   e.printStackTrace();
    // }
    // }
  }

  // @Override
  // public void finish() {
  //   for (PrintWriter conn : connections.values()) {
  //     conn.flush();
  //   }
  // }
}
