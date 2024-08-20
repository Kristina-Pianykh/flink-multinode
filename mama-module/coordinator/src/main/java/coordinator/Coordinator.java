package com.huberlin.coordinator;

import com.huberlin.event.ControlEvent;
import com.huberlin.javacep.communication.addresses.TCPAddressString;
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
    cmdlineOpts.addOption(new Option("n", true, "Number of nodes"));
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
      HashMap<Integer, TCPAddressString> addressBook, int nNodes) {
    HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap = new HashMap<>();

    while (socketWriterMap.size() < nNodes) {
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
    return socketWriterMap;
  }

  public static void main(String[] args) {
    Integer nNodes = null;

    CommandLine cmd = parseArgs(args);
    String addressBookPath = cmd.getOptionValue("addressBook");
    String nNodesStr = cmd.getOptionValue("n");

    try {
      assert addressBookPath != null;
    } catch (AssertionError e) {
      LOG.error("Address book path is not provided.");
      System.exit(1);
    }
    try {
      assert nNodesStr != null;
    } catch (AssertionError e) {
      LOG.error("Number of nodes is not provided.");
      System.exit(1);
    }

    try {
      nNodes = Integer.parseInt(nNodesStr);
    } catch (NumberFormatException e) {
      LOG.error("Failed to parse number of nodes arg {} with error: {}", nNodesStr, e.getMessage());
      System.exit(1);
    }

    HashMap<Integer, TCPAddressString> addressBook = parseAddressBookTCP(addressBookPath, nNodes);
    LOG.info("Parsed address book successfully: {}", addressBook);

    HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap =
        initSocketWriterMap(addressBook, nNodes);

    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      LOG.info("Coordinator started. Listening for connections on port " + PORT + "...");
      while (true) {
        Socket socket = serverSocket.accept();
        new ClientHandler(socket, socketWriterMap).start();
      }
    } catch (IOException e) {
      LOG.error("Failed to start coordinator with error: {}", e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static Long addSeconds(Long timestamp, int seconds) {
    return timestamp + seconds * 1_000_000L;
  }

  private static class ClientHandler extends Thread {
    private Socket socket;
    private HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap;

    public ClientHandler(
        Socket socket, HashMap<Integer, Tuple2<Socket, PrintWriter>> socketWriterMap) {
      this.socket = socket;
      this.socketWriterMap = socketWriterMap;
    }

    private void broadcastControlEvent(ControlEvent controlEvent) {
      for (Tuple2<Socket, PrintWriter> socketWriter : this.socketWriterMap.values()) {
        socketWriter.f1.println(controlEvent.toString());
      }
    }

    @Override
    public void run() {
      try {
        BufferedReader input =
            new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        String client_address = socket.getRemoteSocketAddress().toString();

        while (true) {
          String message = input.readLine();

          if (message == null || message.contains("end-of-the-stream")) {
            LOG.info("Reached the end of the stream for " + client_address + "\n");
            if (message == null) LOG.info("Stream terminated without end-of-the-stream marker.");
            input.close();
            socket.close();
            return;

          } else if (message.startsWith("control")) {
            Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
            assert controlEvent.isPresent() : "Parsing control event " + message + "failed";
            assert controlEvent.get().driftTimestamp.isPresent() : "Shift timestamp is not present";
            LOG.info("Received control event: " + controlEvent.get().toString());

            Long driftTimestamp = controlEvent.get().driftTimestamp.get();
            Long shiftTimestamp = addSeconds(driftTimestamp, TRANSITION_DURATION_IN_SECONDS);
            LOG.info("Drift timestamp: " + driftTimestamp + " Shift timestamp: " + shiftTimestamp);

            ControlEvent newControlEvent =
                new ControlEvent(Optional.of(driftTimestamp), Optional.of(shiftTimestamp));
            broadcastControlEvent(newControlEvent);
            LOG.info("Broadcasted control event: " + newControlEvent.toString());
          }
        }
      } catch (IOException e) {
        LOG.error("Client disconnected with error {}", e.getMessage());
        try {
          socket.close();
        } catch (IOException exc) {
          LOG.error("Failed to close socket with error {}", exc.getMessage());
        }
      }
    }
  }
}
