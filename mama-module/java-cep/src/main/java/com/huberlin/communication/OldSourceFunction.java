package com.huberlin.javacep.communication;

import com.huberlin.event.*;
import com.huberlin.event.ControlEvent;
import com.huberlin.event.Event;
import com.huberlin.event.Message;
import com.huberlin.event.SimpleEvent;
import com.huberlin.javacep.ScheduledTask;
import com.huberlin.javacep.config.NodeConfig;
import com.huberlin.monitor.TimeUtils;
import java.io.*;
import java.net.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OldSourceFunction extends RichSourceFunction<Tuple2<Integer, Event>> {
  private static final Logger LOG = LoggerFactory.getLogger(OldSourceFunction.class);
  public NodeConfig config;
  private volatile boolean isCancelled = false;
  public static boolean multiSinkQueryEnabled = false;
  public static Optional<Long> driftTimestamp = Optional.empty();
  public static Optional<Long> shiftTimestamp = Optional.empty();
  private BlockingQueue<Tuple2<Integer, Message>> parsedMessageStream;
  public Set<Event> partEventBuffer = new HashSet<>();

  // private final int port;

  public OldSourceFunction(NodeConfig config) {
    super();
    this.config = config;
  }

  public static Long getTimeDeltaInSec(Long shiftTimestamp, Long driftTimestamp) {
    return (shiftTimestamp - driftTimestamp) / 1_000_000L;
  }

  @Override
  public void run(SourceFunction.SourceContext<Tuple2<Integer, Event>> sourceContext)
      throws Exception {
    long timestamp_us = Long.MIN_VALUE; // AKA current global watermark
    while (!isCancelled) {

      // retrieve and remove the head of the queue (event stream)
      Tuple2<Integer, Message> srcNodeIdMessage = parsedMessageStream.take();
      // process it with Flink
      timestamp_us =
          Math.max(
              LocalTime.now().toNanoOfDay() / 1000L,
              timestamp_us + 1); // ensures t_us is strictly increasing

      LOG.debug(
          "retrieved the message from parsedMessageStream: {}", srcNodeIdMessage.f1.toString());
      LOG.debug("message is of type {}", srcNodeIdMessage.f1.getClass());
      List<Class> eventClasses = Arrays.asList(SimpleEvent.class, ComplexEvent.class, Event.class);
      if (eventClasses.contains(srcNodeIdMessage.f1.getClass())) {

        /////////// DEBUGGING////////////
        // try {
        //   ScheduledTask scheduledPartEventBufferFlush =
        //       new ScheduledTask(
        //           this.partEventBuffer,
        //           this.addressBook,
        //           this.updatedFwdTable,
        //           this.nodeId,
        //           this.rateMonitoringInputs.partitioningInput);
        //   ScheduledExecutorService scheduledExecutorService =
        // Executors.newScheduledThreadPool(1);
        //   scheduledExecutorService.schedule(
        //       scheduledPartEventBufferFlush, 5, java.util.concurrent.TimeUnit.SECONDS);
        //   LOG.info("Scheduled the task to run in 5 seconds");
        //   scheduledExecutorService.shutdown();
        // } catch (Exception e) {
        //   e.printStackTrace();
        // }
        /////////// DEBUGGING////////////

        Tuple2<Integer, Event> srcNodeIdEvent =
            new Tuple2<>(srcNodeIdMessage.f0, (Event) srcNodeIdMessage.f1);
        sourceContext.collectWithTimestamp(srcNodeIdEvent, timestamp_us);
        sourceContext.emitWatermark(new Watermark(timestamp_us));
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "From node "
                  + srcNodeIdMessage.f0
                  + ": "
                  + "Event "
                  + srcNodeIdMessage.f1
                  + " collected with timestamp "
                  + timestamp_us);
          LOG.trace("Watermark: " + timestamp_us);
        }

        boolean beforeShiftTime =
            TimeUtils.getCurrentTimeInMicroseconds()
                < shiftTimestamp.orElse(
                    0L); // 0L for when timestampts are not set yet and to avoid yet another if-else
        boolean isTransitionPhase =
            driftTimestamp.isPresent()
                && (!shiftTimestamp.isPresent() || (shiftTimestamp.isPresent() && beforeShiftTime));
        LOG.debug("beforeShiftTime = {}", beforeShiftTime);
        LOG.debug("driftTimestamp.isPresent() = {}", driftTimestamp.isPresent());
        LOG.debug("shiftTimestamp.isPresent() = {}", shiftTimestamp.isPresent());
        LOG.debug("isTransitionPhase = {}", isTransitionPhase);

        if (isTransitionPhase) {
          // TODO: make sure the buffer is cleared for GC after the shift (it's cleared in the
          // timerTask tho)
          LOG.info(
              "rateMonitoringInputs.isNonFallbackNode(this.nodeId) = "
                  + config.rateMonitoringInputs.isNonFallbackNode(config.nodeId));
          LOG.info(
              "event.eventType.equals(rateMonitoringInputs.partitioningInput) = "
                  + srcNodeIdEvent.f1.eventType.equals(
                      config.rateMonitoringInputs.partitioningInput));

          if (config.rateMonitoringInputs.isNonFallbackNode(config.nodeId)
              && srcNodeIdEvent.f1.eventType.equals(
                  config.rateMonitoringInputs.partitioningInput)) {
            this.partEventBuffer.add(srcNodeIdEvent.f1);
            LOG.info("Inserted event {} into the nonPartBuffer", srcNodeIdEvent.f1);
          }
        }

      } else if (srcNodeIdMessage.f1.getClass().equals(ControlEvent.class)) {
        LOG.debug("message is of type ControlEvent");
        ControlEvent controlEvent = (ControlEvent) srcNodeIdMessage.f1;

        // driftimeTimestamp should always be set? assert?
        if (controlEvent.driftTimestamp.isPresent() && !driftTimestamp.isPresent()) {
          driftTimestamp = Optional.of(controlEvent.driftTimestamp.get());
          LOG.info("Drift timestamp: " + TimeUtils.format(driftTimestamp.get()));

        } else if (controlEvent.shiftTimestamp.isPresent() && !shiftTimestamp.isPresent()) {
          shiftTimestamp = Optional.of(controlEvent.shiftTimestamp.get());
          Long transitionDurationInSec =
              getTimeDeltaInSec(shiftTimestamp.get(), driftTimestamp.get());
          LOG.info("Drift timestamp: {}", TimeUtils.format(driftTimestamp.get()));
          LOG.info("Shift timestamp: {}", TimeUtils.format(shiftTimestamp.get()));
          LOG.info("Transition duration: {} seconds", transitionDurationInSec);

          try {
            ScheduledTask scheduledPartEventBufferFlush =
                new ScheduledTask(this.partEventBuffer, this.config);
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.schedule(
                scheduledPartEventBufferFlush,
                transitionDurationInSec,
                java.util.concurrent.TimeUnit.SECONDS);
            LOG.info("Scheduled the task to run in {} seconds", transitionDurationInSec);
            scheduledExecutorService.shutdown();
          } catch (Exception e) {
            LOG.error("Failed to initialize a scheduled task");
            e.printStackTrace();
          }
        }
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
    parsedMessageStream = new LinkedBlockingQueue<>();

    Thread new_connection_accepting_thread =
        new Thread(
            () -> {
              try (ServerSocket accepting_socket = new ServerSocket(this.config.hostAddress.port)) {
                // accepting_socket.setReuseAddress(true);
                // try {
                //   accepting_socket.bind(new InetSocketAddress(port));
                //
                // } catch (java.net.BindException e) {
                //   log.error("Failed to bind server socket with the endpoint" + port + ": " + e);
                //   e.printStackTrace(System.err);
                //   System.exit(1);
                // }
                // LOG.debug("Bound server socket " + accepting_socket);
                while (!isCancelled) {
                  try {
                    Socket new_client = accepting_socket.accept();
                    new_client.shutdownOutput(); // one-way data connection (only read)
                    new Thread(() -> handleClient(new_client)).start();
                    LOG.info("New client connected: " + new_client);
                  } catch (IOException e) {
                    LOG.warn("Exception in connection-accepting thread: " + e);
                  }
                }
                LOG.debug("Not accepting additional connections.");
              } catch (IOException e) {
                LOG.error("Failed to open server socket: " + e);
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
        LOG.info("Received message: " + message);

        // check, if a client has sent his entire event stream..
        if (message.contains("end-of-the-stream")) {
          LOG.info("Reached the end of the stream for " + client_address);
          input.close();
          socket.close();
          return;

        } else if (message == null) {
          LOG.warn("Stream terminated without end-of-the-stream marker.");
          // do nothing, let the client reconnect after a one-time buffer flush
          // for regular event forwarding

        } else if (message.startsWith("control")) {
          Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
          try {
            assert controlEvent.isPresent();
          } catch (AssertionError e) {
            LOG.error("Parsing control event {} failed", message);
            e.printStackTrace();
          }
          parsedMessageStream.put(new Tuple2<>(null, controlEvent.get()));

        } else if (client_node_id == null) {
          if (!message.startsWith("I am ")) {
            LOG.error(
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
          LOG.info("Client " + client_address + " introduced itself as node " + client_node_id);

        } else if (message.contains("|")) {
          Event event = Event.parse(message);
          parsedMessageStream.put(new Tuple2<>(client_node_id, event));

        } else {
          LOG.debug("Ignoring message:" + message);
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.info("Client disconnected");
      try {
        socket.close();
      } catch (IOException exc) {
        LOG.error("Failed to close the socket {} with error {}", socket.toString(), exc);
        e.printStackTrace();
      }
    }
  }
}
