package com.huberlin.coordinator;

import com.huberlin.event.ControlEvent;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.monitor.TimeUtils;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator {
  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final int NODE_NUM = 1;
  private static final int PORT = 6668;
  private static final int COORDINATOR_ID = 42069;
  private static final int RETRY_INTERVAL = 3000;
  private static final int TRANSITION_DURATION_IN_SECONDS = 8;

  private static CommandLine parseArgs(String[] args) {
    final Options cmdlineOpts = new Options();
    final HelpFormatter formatter = new HelpFormatter();
    final String usage =
        "java -ea --add-opens java.base/java.lang=ALL-UNNAMED \\\n"
            + " --add-opens=java.base/java.net=ALL-UNNAMED \\\n"
            + " --add-opens=java.base/java.util=ALL-UNNAMED \\\n"
            + " -jar $JAR \\\n";
    cmdlineOpts.addOption(new Option("addressBook", true, "Path to the address book"));
    final CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(cmdlineOpts, args);
    } catch (ParseException e) {
      LOG.error("Failed to parse command line arguments with error: {}", e.getMessage());
      formatter.printHelp(usage, cmdlineOpts);
      System.exit(1);
    }
    return null;
  }

  private static HashMap<Integer, TCPAddressString> parseAddressBookTCP(String path, int nodeNum) {
    HashMap<Integer, TCPAddressString> addressBookTCP = new HashMap<>();

    try {
      String jsonString = new String(Files.readAllBytes(Paths.get(path)));
      JSONObject jsobObj = new JSONObject(jsonString);
      for (String nodeIdAsStr : jsobObj.keySet()) {
        int nodeId = Integer.parseInt((nodeIdAsStr).trim());

        if (nodeId >= nodeNum) continue;

        String endpoint = jsobObj.getString(nodeIdAsStr);
        if (!endpoint.contains(":")) {
          LOG.error("Endpoint for Node ID " + nodeIdAsStr + "in address book does not contain ':'");
          System.exit(-1);
        }
        addressBookTCP.put(nodeId, new TCPAddressString(endpoint.trim()));
      }
    } catch (IOException e) {
      LOG.error("Failed to read address book file with error: {}", e.getMessage());
      System.exit(-1);
    }
    return addressBookTCP;
  }

  public static HashMap<Integer, Tuple2<Socket, PrintWriter>> initSocketWriterMap(
      HashMap<Integer, TCPAddressString> addressBook) {
    HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap = new HashMap<>();

    while (socketWriterMap.size() < NODE_NUM) {
      for (Map.Entry<Integer, TCPAddressString> entry : addressBook.entrySet()) {
        int dstNodeId = entry.getKey();
        if (socketWriterMap.containsKey(dstNodeId)) continue;

        TCPAddressString dst = entry.getValue();
        String host = dst.getHost();
        // LOG.info("Host: {}", host);
        int port = dst.getPort();
        // LOG.info("Port: {}", port);

        try {
          Socket client_socket = new Socket(host, port);
          PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
          writer.println("I am " + COORDINATOR_ID);

          LOG.info("Connection for to " + dst + " established");
          socketWriterMap.put(dstNodeId, new Tuple2<>());
          socketWriterMap.get(dstNodeId).f0 = client_socket;
          socketWriterMap.get(dstNodeId).f1 = writer;
        } catch (ConnectException e) {
          LOG.warn("Server is not responding: {}", e.getMessage());
        } catch (IOException e) {
          LOG.error("Failed to establish connection with error: {}", e.getMessage());
        }
      }

      try {
        Thread.sleep(RETRY_INTERVAL);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted with error: {}", e.getMessage());
        e.printStackTrace();
      }
    }

    try {
      assert socketWriterMap.size() == NODE_NUM;
    } catch (AssertionError e) {
      LOG.error("Failed to establish connections with all nodes.");
      System.exit(-1);
    }

    return socketWriterMap;
  }

  public static void main(String[] args) {
    CommandLine cmd = parseArgs(args);
    String addressBookPath = cmd.getOptionValue("addressBook");

    try {
      assert addressBookPath != null;
    } catch (AssertionError e) {
      LOG.error("Address book path is not provided.");
      System.exit(1);
    }
    HashMap<Integer, TCPAddressString> addressBook = parseAddressBookTCP(addressBookPath, NODE_NUM);
    LOG.info("Parsed address book successfully: {}", addressBook);

    HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap =
        initSocketWriterMap(addressBook);
    try {
      Socket socket = socketWriterMap.get(0).f0;
      PrintWriter writer = socketWriterMap.get(0).f1;
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted with error: {}", e.getMessage());
        e.printStackTrace();
      }
      Long driftTimestamp = TimeUtils.getCurrentTimeInMicroseconds();
      ControlEvent controlEvent = new ControlEvent(Optional.of(driftTimestamp), Optional.empty());
      writer.println(controlEvent.toString());
      LOG.info("Sent control event: {}", controlEvent.toString());

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted with error: {}", e.getMessage());
        e.printStackTrace();
      }
      controlEvent =
          new ControlEvent(
              Optional.of(driftTimestamp), Optional.of(TimeUtils.getCurrentTimeInMicroseconds()));
      writer.println(controlEvent.toString());
      LOG.info("Sent control event: {}", controlEvent.toString());
      writer.println("end-of-the-stream\n");
      writer.close();
      socket.close();
    } catch (IOException e) {
      LOG.error("Failed to establish connection from coordinator: {}", e.getMessage());
      e.printStackTrace();
    }
  }

  public static void broadcastControlEvent(ControlEvent controlEvent, PrintWriter writer) {
    LOG.info("Broadcasting control event {}", controlEvent.toString());
    writer.println(controlEvent.toString());
  }

  public static Long addSeconds(Long timestamp, int seconds) {
    return timestamp + seconds * 1_000_000L;
  }

  // private static class ClientHandler extends Thread {
  //   private ControlEvent controlEvent;
  //   private static AtomicBoolean triggerFired = new AtomicBoolean(false);
  //
  //   public ClientHandler(Socket socket, PrintWriter writer, ControlEvent controlEvent) {
  //     this.controlEvent = controlEvent;
  //   }
  //
  //   @Override
  //   public void run() {
  //     if (message == null || message.contains("end-of-the-stream")) {
  //       LOG.info("Reached the end of the stream for " + client_address + "\n");
  //       if (message == null) LOG.info("Stream terminated without end-of-the-stream marker.");
  //       input.close();
  //       socket.close();
  //       return;
  //
  //     } else if (message.startsWith("control")) {
  //       Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
  //       assert controlEvent.isPresent() : "Parsing control event " + message + "failed";
  //       assert controlEvent.get().driftTimestamp.isPresent() : "Shift timestamp is not present";
  //       LOG.info("Received control event: " + controlEvent.get().toString());
  //
  //       Long driftTimestamp = controlEvent.get().driftTimestamp.get();
  //       Long shiftTimestamp = addSeconds(driftTimestamp, TRANSITION_DURATION_IN_SECONDS);
  //       LOG.info("Drift timestamp: " + driftTimestamp + " Shift timestamp: " + shiftTimestamp);
  //
  //       ControlEvent newControlEvent =
  //           new ControlEvent(Optional.of(driftTimestamp), Optional.of(shiftTimestamp));
  //       broadcastControlEvent(newControlEvent);
  //       triggerFired.set(true);
  //       LOG.info("Broadcasted control event: " + newControlEvent.toString());
  //     }
  //   }
  // }
}
