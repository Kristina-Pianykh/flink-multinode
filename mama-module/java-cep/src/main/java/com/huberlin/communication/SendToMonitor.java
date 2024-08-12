package com.huberlin.javacep.communication;

import com.huberlin.event.Event;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendToMonitor extends RichSinkFunction<Event> {
  private static final Logger LOG = LoggerFactory.getLogger(TCPEventSender.class);
  final int nodeId;
  final boolean isMultiSinkNode;
  private PrintWriter writer = null;
  private Socket socket = null;
  private final int port;

  public SendToMonitor(int nodeId, int nodePort, boolean isMiltiSinkNode) {
    this.nodeId = nodeId;
    this.port = nodePort + 20;
    this.isMultiSinkNode = isMiltiSinkNode;
    LOG.info("SendToMonitor created with port " + port);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (!isMultiSinkNode) {
      LOG.info("SendToMonitor: Not a multi sink node. Skip creating a sink.");
      return;
    }
    this.socket = openSocket(port, 1);
    if (this.socket == null) {
      return;
    }
    this.writer = createWriter(this.socket);
    if (this.writer == null) {
      return;
    }
    assert this.socket != null;
    assert this.writer != null;
    LOG.info("SendToMonitor: Opened socket on port " + port);
  }

  public PrintWriter createWriter(Socket socket) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(socket.getOutputStream(), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return writer;
  }

  public Socket openSocket(int port, int attempt) {
    Socket socket = null;
    if (attempt > 5) {
      LOG.error("Could not open socket on port " + port);
      return null;
    }
    try {
      socket = new Socket("localhost", port);

      LOG.info("Socket opened on port " + port);
    } catch (IOException e) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      LOG.warn("Could not open socket on port {}. Trying again in 1 second...", port);
      socket = openSocket(port, attempt + 1);
    }
    return socket;
  }

  /** Called by flink to send events */
  @Override
  public void invoke(Event event, Context ignored) {
    if (this.socket == null && this.writer == null) {
      return;
    }
    if (!isMultiSinkNode) {
      return;
    }
    LOG.info("SendToMonitor: " + event.toString());
    this.writer.println(event.toString());
  }

  @Override
  public void close() {
    if (!isMultiSinkNode) {
      return;
    }
    try {
      this.writer.close();
      this.socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // @Override
  // public void finish() {
  //   for (PrintWriter conn : connections.values()) {
  //     conn.flush();
  //   }
  // }
}
