package com.huberlin.javacep.communication;

import com.huberlin.event.ControlEvent;
import com.huberlin.event.Event;
import com.huberlin.javacep.ScheduledTask;
import com.huberlin.javacep.config.ForwardingTable;
import com.huberlin.javacep.config.NodeAddress;
import com.huberlin.monitor.FormatTimestamp;
import com.huberlin.sharedconfig.RateMonitoringInputs;
import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OldSourceFunction extends RichSourceFunction<Tuple2<Integer, Event>> {
  private static final Logger log = LoggerFactory.getLogger(OldSourceFunction.class);
  public RateMonitoringInputs rateMonitoringInputs;
  public Map<Integer, NodeAddress> addressBook;
  public ForwardingTable updatedFwdTable;
  public int nodeId;
  private volatile boolean isCancelled = false;
  public static boolean multiSinkQueryEnabled = false;
  public static Optional<Long> driftTimestamp = Optional.empty();
  public static Optional<Long> shiftTimestamp = Optional.empty();
  public static Optional<Date> _shiftTimestamp = Optional.empty();
  private BlockingQueue<Tuple2<Integer, Event>> merged_event_stream;
  public Set<Event> partEventBuffer = new HashSet<>();
  private final int port;

  public OldSourceFunction(
      RateMonitoringInputs rateMonitoringInputs,
      Map<Integer, NodeAddress> addressBook,
      ForwardingTable updatedFwdTable,
      int nodeId,
      int listen_port) {
    super();
    this.rateMonitoringInputs = rateMonitoringInputs;
    this.addressBook = addressBook;
    this.updatedFwdTable = updatedFwdTable;
    this.nodeId = nodeId;
    this.port = listen_port;
  }

  @Override
  public void run(SourceFunction.SourceContext<Tuple2<Integer, Event>> sourceContext)
      throws Exception {
    long timestamp_us = Long.MIN_VALUE; // AKA current global watermark
    while (!isCancelled) {
      // retrieve and remove the head of the queue (event stream)
      Tuple2<Integer, Event> event_with_source_node_id = merged_event_stream.take();
      // process it with Flink
      timestamp_us =
          Math.max(
              LocalTime.now().toNanoOfDay() / 1000L,
              timestamp_us + 1); // ensures t_us is strictly increasing
      sourceContext.collectWithTimestamp(event_with_source_node_id, timestamp_us);
      sourceContext.emitWatermark(new Watermark(timestamp_us));
      if (log.isTraceEnabled()) {
        log.trace(
            "From node "
                + event_with_source_node_id.f0
                + ": "
                + "Event "
                + event_with_source_node_id.f1
                + " collected with timestamp "
                + timestamp_us);
        log.trace("Watermark: " + timestamp_us);
      }
    }
  }

  @Override
  public void cancel() {
    isCancelled = true;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    merged_event_stream = new LinkedBlockingQueue<>();

    // TODO: do we need server socket initialization in a thread?
    Thread new_connection_accepting_thread =
        new Thread(
            () -> {
              try (ServerSocket accepting_socket = new ServerSocket(port)) {
                // accepting_socket.setReuseAddress(true);
                // try {
                //   accepting_socket.bind(new InetSocketAddress(port));
                //
                // } catch (java.net.BindException e) {
                //   log.error("Failed to bind server socket with the endpoint" + port + ": " + e);
                //   e.printStackTrace(System.err);
                //   System.exit(1);
                // }
                log.debug("Bound server socket " + accepting_socket);
                while (!isCancelled) {
                  try {
                    Socket new_client = accepting_socket.accept();
                    new_client.shutdownOutput(); // one-way data connection (only read)
                    new Thread(() -> handleClient(new_client)).start();
                    log.info("New client connected: " + new_client);
                  } catch (IOException e) {
                    log.warn("Exception in connection-accepting thread: " + e);
                  }
                }
                log.debug("Not accepting additional connections.");
              } catch (IOException e) {
                log.error("Failed to open server socket: " + e);
                e.printStackTrace(System.err);
                System.exit(1);
              }
            });
    new_connection_accepting_thread.start();
  }

  public void handleClient(Socket socket) {
    try {
      BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      String client_address = socket.getRemoteSocketAddress().toString();
      Integer client_node_id = null;
      while (!isCancelled) {
        String message = input.readLine();

        // check, if a client has sent his entire event stream..
        // TODO: double check the same condition: separate
        if (message == null || message.contains("end-of-the-stream")) {
          log.info("Reached the end of the stream for " + client_address);
          if (message == null) log.warn("Stream terminated without end-of-the-stream marker.");
          input.close();
          socket.close(); // sockets connections are not reused
          return;

        } else if (message.startsWith("control")) {
          Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
          assert controlEvent.isPresent() : "Parsing control event " + message + "failed";
          System.out.println("Received control event: " + controlEvent.get().toString());

          if (controlEvent.get().driftTimestamp.isPresent() && driftTimestamp.isEmpty()) {
            driftTimestamp = Optional.of(controlEvent.get().driftTimestamp.get());
            LocalDateTime secondsLater = LocalDateTime.now().plusSeconds(5);
            Date secondsLaterAsDate =
                Date.from(secondsLater.atZone(ZoneId.systemDefault()).toInstant());
            System.out.println("Drift timestamp: " + FormatTimestamp.format(driftTimestamp.get()));
            System.out.println("Current time: " + LocalDateTime.now().toString());
            System.out.println("Shift timestamp: " + secondsLaterAsDate.toString());
            Timer timer = new Timer();
            System.out.println("Initialized a timer");
            System.out.println("partEventBuffer = " + partEventBuffer.toString());
            System.out.println("addressBook = " + addressBook.toString());
            System.out.println("updatedFwdTable = " + updatedFwdTable.toString());
            System.out.println("nodeId = " + nodeId);
            System.out.println(
                "rateMonitoringInputs.partitioningInput = "
                    + rateMonitoringInputs.partitioningInput);
            ScheduledTask scheduledPartEventBufferFlush =
                new ScheduledTask(
                    this.partEventBuffer,
                    this.addressBook,
                    this.updatedFwdTable,
                    this.nodeId,
                    this.rateMonitoringInputs.partitioningInput,
                    timer);
            System.out.println("Initialized a scheduled task");

            timer.schedule(scheduledPartEventBufferFlush, secondsLaterAsDate);
            // System.out.println("Fire time: " + secondsLaterAsDate.toString());
            _shiftTimestamp = Optional.of(secondsLaterAsDate);
          }

        } else if (client_node_id == null) {
          if (!message.startsWith("I am ")) {
            log.error(
                "The first line from a newly connected client must be 'I am '"
                    + " followed by the client's node_id.\n"
                    + " Received: "
                    + message
                    + "\n from remote socket: "
                    + client_address);
            input.close();
            socket.close();
            return;
          }
          client_node_id = Integer.valueOf(message.substring(5));
          log.info("Client " + client_address + " introduced itself as node " + client_node_id);
        } else if (message.contains("|")) {
          Event event = Event.parse(message);
          merged_event_stream.put(new Tuple2<>(client_node_id, event));
          LocalTime currTime = LocalTime.now();
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
          System.out.println(event + " was received at: " + currTime.format(formatter));

          if (_shiftTimestamp.isPresent()) {
            LocalDateTime fireTime =
                _shiftTimestamp.get().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            boolean isTransitionPhase =
                driftTimestamp.isPresent()
                    && _shiftTimestamp.isPresent()
                    && LocalDateTime.now().isBefore(fireTime);

            // TODO: make sure the buffer is cleared for GC after the shift (it's cleared in the
            // timerTask tho)
            System.out.println(
                "rateMonitoringInputs.isNonFallbackNode(this.nodeId) = "
                    + rateMonitoringInputs.isNonFallbackNode(this.nodeId));
            System.out.println(
                "event.eventType.equals(rateMonitoringInputs.partitioningInput) = "
                    + event.eventType.equals(rateMonitoringInputs.partitioningInput));
            System.out.println("isTransitionPhase = " + isTransitionPhase);
            if (rateMonitoringInputs.isNonFallbackNode(this.nodeId)
                && event.eventType.equals(rateMonitoringInputs.partitioningInput)
                && isTransitionPhase) this.partEventBuffer.add(event);
          }

        } else {
          log.debug("Ignoring message:" + message);
        }
      }
    } catch (IOException | InterruptedException e) {
      log.info("Client disconnected");
      try {
        socket.close();
      } catch (IOException ignored) {
      }
    }
  }
}
