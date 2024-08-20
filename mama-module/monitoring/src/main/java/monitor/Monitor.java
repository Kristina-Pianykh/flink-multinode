package com.huberlin.monitor;

import com.huberlin.event.Event;
import com.huberlin.sharedconfig.RateMonitoringInputs;
import java.io.*;
import java.net.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor {
  static {
    // Load log4j.properties from the classpath
    Configurator.initialize(
        null, Monitor.class.getClassLoader().getResource("log4j2.xml").getPath());
  }

  private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

  private static CommandLine parse_cmdline_args(String[] args) {
    final Options cmdline_opts = new Options();
    final HelpFormatter formatter = new HelpFormatter();
    cmdline_opts.addOption(
        new Option("addressbook", true, "Path to the global configuration file"));
    cmdline_opts.addOption(new Option("node", true, "Node ID on which the monitor is running"));
    cmdline_opts.addOption(
        new Option(
            "monitoringinputs",
            true,
            "Path to the directory with the inputs for computing the inequality"));
    cmdline_opts.addOption(
        new Option("applyStrategy", false, "Flag whether to apply the adaptive strategy"));
    cmdline_opts.addOption(new Option("outputdir", true, "Path to the output directory"));
    final CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(cmdline_opts, args);
    } catch (ParseException e) {
      LOG.error("Failed to parse command line arguments: {}", e.getMessage());
      formatter.printHelp("java -jar cep-node.jar", cmdline_opts);
      System.exit(1);
    }
    return null;
  }

  public static void main(String[] args) {
    HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates = new HashMap<>();
    int queueSize = 1000;
    int coordinatorPort = 6668;
    Integer monitorPort = null;
    Integer nodePort = null;
    final int socketTimeOutMillis = 660000;
    boolean applyStrategy = false;

    CommandLine cmd = parse_cmdline_args(args);
    String addressBookPath =
        cmd.getOptionValue(
            "addressbook",
            "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json");
    String outputDir = cmd.getOptionValue("outputdir");

    try {
      assert outputDir != null;
    } catch (AssertionError e) {
      LOG.error("Output directory is not set. Exiting...");
      System.exit(-1);
    }
    if (!(new File(outputDir).isDirectory())) {
      LOG.error("Output directory does not exist. Exiting...");
      System.exit(-1);
    }
    if (!outputDir.endsWith("/")) outputDir = outputDir + "/";

    if (cmd.hasOption("applyStrategy")) applyStrategy = true;
    LOG.info("Apply adaptive strategy: {}", applyStrategy);

    String rateMonitoringInputsPath = cmd.getOptionValue("monitoringinputs");
    RateMonitoringInputs rateMonitoringInputs =
        RateMonitoringInputs.parseRateMonitoringInputs(rateMonitoringInputsPath);
    LOG.info("rateMonitoringInputs: {}", rateMonitoringInputs.toString());

    String nodeId = cmd.getOptionValue("node");
    boolean isMultiSinkNode =
        rateMonitoringInputs.multiSinkNodes.contains(Integer.parseInt(nodeId));
    if (!isMultiSinkNode) {
      LOG.warn("Node {} is not a multi-sink node. Exiting...", nodeId);
      System.exit(0);
    }

    try {
      nodePort = JsonParser.getNodePort(addressBookPath, nodeId);
      assert nodePort != null : "Failed to parse node port from address book.";
      monitorPort = nodePort + 20;
      LOG.info("Node port: {}; monitor port: {}", nodePort, monitorPort);
    } catch (IOException e) {
      LOG.error("Failed to parse node port from address book: {}", e.getMessage());
      System.exit(-1);
    }

    BlockingEventBuffer buffer = new BlockingEventBuffer(queueSize);
    new Thread(
            new MonitoringData(
                buffer,
                rateMonitoringInputs,
                totalRates,
                Integer.parseInt(nodeId),
                nodePort,
                coordinatorPort,
                applyStrategy))
        .start();

    // Thread fileWriter = new Thread(new FileWrite(Integer.parseInt(nodeId), totalRates));
    // fileWriter.start();

    try (ServerSocket serverSocket = new ServerSocket(monitorPort)) {
      LOG.info("Server started. Listening for connections on port " + monitorPort + "...");
      while (true) {
        Socket socket = serverSocket.accept();
        socket.setSoTimeout(socketTimeOutMillis); // in milies; 11 min
        new ClientHandler(nodeId, socket, buffer, rateMonitoringInputs, totalRates, outputDir)
            .start();
        LOG.info("Main function, buffer size: " + buffer.size());
      }
    } catch (IOException e) {
      LOG.error("Server exception: {}", e.getMessage());
      System.exit(-1);
    }
  }

  private static class ClientHandler extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);
    String nodeId;
    private Socket socket;
    private BlockingEventBuffer buffer;
    private RateMonitoringInputs rateMonitoringInputs;
    private HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates;
    private final File filePath;

    public ClientHandler(
        String nodeId,
        Socket socket,
        BlockingEventBuffer buffer,
        RateMonitoringInputs rateMonitoringInputs,
        HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates,
        String outputDir) {
      this.nodeId = nodeId;
      this.socket = socket;
      this.buffer = buffer;
      this.rateMonitoringInputs = rateMonitoringInputs;
      this.totalRates = totalRates;
      this.filePath = new File(outputDir + "totalRates" + this.nodeId + ".csv");

      // Initialize the CSV file with headers
      try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
        out.println("event_type,timestamp,timestamp_long");
      } catch (IOException e) {
        LOG.error("Error initializing CSV file: {}", e.getMessage());
      }
    }

    @Override
    public void run() {
      try {
        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        LOG.info(
            "Socket for the connection: {}:{} is open.", socket.getInetAddress(), socket.getPort());
        Event event;
        while (true) {
          try {
            String message = input.readLine();
            if (message == null) {
              LOG.warn("Client has closed the connection.");
              break;
            }
            LOG.info("Received message: {}", message);
            event = Event.parse(message);
            // System.out.println(message.contains("|"));
            if (message.contains("|")) {
              event = Event.parse(message);
              LocalTime currTime = LocalTime.now();
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
              LOG.info("{} was received at: {}", event, currTime.format(formatter));
              writeEventToCSV(
                  event.getEventType(), currTime.format(formatter), event.getTimestamp());

              boolean insertSucess = false;
              boolean relevantEvent =
                  (rateMonitoringInputs.nonPartitioningInputs.contains(event.getEventType())
                      || rateMonitoringInputs.partitioningInput.equals(event.getEventType())
                      || rateMonitoringInputs.multiSinkQuery.equals(event.getEventType()));
              if (!relevantEvent) {
                LOG.warn("Ignoring irrelevant event: {}", event);
                continue;
              }
              while (!insertSucess) {
                try {
                  insertSucess = buffer.offer(event, 1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }

                if (!insertSucess) {
                  LOG.warn("Failed to insert event into the buffer. Resizing the buffer...");
                  buffer.resize();
                } else {
                  LOG.info(
                      "Successfully inserted event into the buffer. Buffer size: {}. Buffer: {}",
                      buffer.size(),
                      buffer.toString());
                  insertSucess = true;
                }
              }

            } else {
              LOG.warn("Ignoring message: {}", message);
            }

          } catch (SocketTimeoutException ex) {
            LOG.warn("Socket timed out!");
            // LOG.info("Stopping the file writer thread...");
            // ((FileWrite) fileWriter).terminate();
            // try {
            //   fileWriter.join();
            //
            // } catch (InterruptedException e) {
            //   LOG.error(
            //       "Joining of fileWriter thread has been interrupted. Error: {}",
            // e.getMessage());
            // }
            // LOG.info("File writer thread stopped.");
            socket.close();
            System.exit(1);

          } catch (EOFException e) {
            LOG.error("Client has closed the connection.");
            // socket.close();
            break; // Exit the loop if EOFException is caught
          }
        }

      } catch (IOException e) {
        LOG.error("Error: {}", e.getMessage());
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    private void writeEventToCSV(String eventType, String timestamp, Long timestampLong) {
      try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
        out.println(
            "\""
                + eventType
                + "\""
                + ","
                + "\""
                + timestamp
                + "\""
                + ","
                + "\""
                + timestampLong
                + "\"");
        LOG.info("Wrote event to CSV: {}, {}", eventType, timestamp);
      } catch (IOException e) {
        LOG.error("Error writing to CSV file: {}", e.getMessage());
      }
    }
  }
}
