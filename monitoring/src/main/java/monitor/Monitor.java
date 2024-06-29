package monitor;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import monitor.event.Event;
import org.apache.commons.cli.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class Monitor {

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
    final CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(cmdline_opts, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("java -jar cep-node.jar", cmdline_opts);
      System.exit(1);
    }
    return null;
  }

  public static RateMonitoringInputs parseRateMonitoringInputs(String filePath) {
    RateMonitoringInputs rateMonitoringInputs = new RateMonitoringInputs();
    try {
      String jsonString = new String(Files.readAllBytes(Paths.get(filePath)));
      JSONObject jsonObject = new JSONObject(jsonString);
      rateMonitoringInputs.multiSinkQuery = jsonObject.getString("multiSinkQuery");
      rateMonitoringInputs.multiSinkNodes =
          jsonArrayToInt(jsonObject.getJSONArray("multiSinkNodes"));
      rateMonitoringInputs.numMultiSinkNodes = jsonObject.getInt("numMultiSinkNodes");
      rateMonitoringInputs.partitioningInput = jsonObject.getString("partitioningInput");
      rateMonitoringInputs.queryInputs = jsonArrayToList(jsonObject.getJSONArray("queryInputs"));
      rateMonitoringInputs.nonPartitioningInputs =
          jsonArrayToList(jsonObject.getJSONArray("nonPartitioningInputs"));
      rateMonitoringInputs.steinerTreeSize = jsonObject.getInt("steinerTreeSize");
      rateMonitoringInputs.numNodesPerQueryInput = new HashMap<>();
      JSONObject numNodesPerQueryInput = jsonObject.getJSONObject("numNodesPerQueryInput");
      for (String key : numNodesPerQueryInput.keySet()) {
        rateMonitoringInputs.numNodesPerQueryInput.put(key, numNodesPerQueryInput.getInt(key));
      }
    } catch (IOException e) {
      System.err.println("Error reading JSON file: " + e.getMessage());
    }
    return rateMonitoringInputs;
  }

  private static List<String> jsonArrayToList(JSONArray jsonArray) {
    return jsonArray.toList().stream().map(Object::toString).collect(Collectors.toList());
  }

  private static List<Integer> jsonArrayToInt(JSONArray jsonArray) {
    return jsonArray.toList().stream()
        .map(Object::toString)
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    int queueSize = 1000;

    CommandLine cmd = parse_cmdline_args(args);
    String addressBookPath =
        cmd.getOptionValue(
            "addressbook",
            "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json"); // global config
    String rateMonitoringInputsPath = cmd.getOptionValue("monitoringinputs"); // local config
    RateMonitoringInputs rateMonitoringInputs = parseRateMonitoringInputs(rateMonitoringInputsPath);
    System.out.println(rateMonitoringInputs.toString());

    String nodeId = cmd.getOptionValue("node");
    boolean isMultiSinkNode =
        rateMonitoringInputs.multiSinkNodes.contains(Integer.parseInt(nodeId));
    if (!isMultiSinkNode) {
      System.out.println("Node " + nodeId + " is not a multi-sink node.");
      System.out.println("Exiting...");
      System.exit(0);
    }

    int monitorPort = -1;
    try {
      monitorPort = JsonParser.getMonitorPort(addressBookPath, nodeId);
      System.out.println("Monitor port: " + monitorPort);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    assert monitorPort > 0;

    BlockingEventBuffer buffer = new BlockingEventBuffer(queueSize);
    new Thread(new MonitoringData(buffer, rateMonitoringInputs)).start();

    try (ServerSocket serverSocket = new ServerSocket(monitorPort)) {
      System.out.println(
          "Server started. Listening for connections on port " + monitorPort + "...");
      while (true) {
        Socket socket = serverSocket.accept();
        new ClientHandler(socket, buffer, rateMonitoringInputs).start();
        System.out.println("Main function, buffer size: " + buffer.size());
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private static class ClientHandler extends Thread {
    private Socket socket;
    private BlockingEventBuffer buffer;
    private RateMonitoringInputs rateMonitoringInputs;

    public ClientHandler(
        Socket socket, BlockingEventBuffer buffer, RateMonitoringInputs rateMonitoringInputs) {
      this.socket = socket;
      this.buffer = buffer;
      this.rateMonitoringInputs = rateMonitoringInputs;
    }

    @Override
    public void run() {
      try {
        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        System.out.println(
            "Socket for the connection: "
                + socket.getInetAddress()
                + ":"
                + socket.getPort()
                + " is open.");
        Event event;
        while (true) {
          try {
            String message = input.readLine();
            System.out.println("Received message: " + message);
            event = Event.parse(message);
            System.out.println(message.contains("|"));
            if (message.contains("|")) {
              event = Event.parse(message);
              LocalTime currTime = LocalTime.now();
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
              System.out.println(event + " was received at: " + currTime.format(formatter));

              boolean insertSucess = false;
              boolean relevantEvent =
                  (rateMonitoringInputs.nonPartitioningInputs.contains(event.getEventType())
                      || rateMonitoringInputs.partitioningInput.equals(event.getEventType())
                      || rateMonitoringInputs.multiSinkQuery.equals(event.getEventType()));
              if (!relevantEvent) {
                System.out.println("Ignoring irrelevant event: " + event);
                continue;
              }
              while (!insertSucess) {
                try {
                  insertSucess = buffer.offer(event, 1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }

                if (!insertSucess) {
                  System.out.println("Failed to insert event into the buffer.");
                  System.out.println("Resizing the buffer...");
                  buffer.resize();
                } else {
                  System.out.println("Successfully inserted event into the buffer.");
                  System.out.println("Buffer size: " + buffer.size());
                  System.out.println(buffer.toString());
                  insertSucess = true;
                }
              }

            } else {
              System.out.println("Ignoring message: " + message);
            }

          } catch (EOFException e) {
            System.out.println("Client has closed the connection.");
            break; // Exit the loop if EOFException is caught
          }
        }

      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}