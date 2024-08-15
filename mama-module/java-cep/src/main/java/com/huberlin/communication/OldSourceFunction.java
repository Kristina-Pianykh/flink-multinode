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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OldSourceFunction extends RichSourceFunction<Tuple2<Integer, Message>> {
  private static final Logger LOG = LoggerFactory.getLogger(OldSourceFunction.class);
  public NodeConfig config;
  public static Optional<Long> driftTimestamp = Optional.empty();
  public static Optional<Long> shiftTimestamp = Optional.empty();
  private BlockingQueue<Tuple2<Integer, Message>> parsedMessageStream;
  public Set<Event> partEventBuffer = new HashSet<>();
  private volatile boolean isCancelled = false;
  public static AtomicBoolean multiSinkQueryEnabled = new AtomicBoolean(true);

  // private final int port;

  public OldSourceFunction(NodeConfig config) {
    super();
    this.config = config;
  }

  public static Long getTimeDeltaInSec(Long shiftTimestamp, Long driftTimestamp) {
    return (shiftTimestamp - driftTimestamp) / 1_000_000L;
  }

  @Override
  public void run(SourceFunction.SourceContext<Tuple2<Integer, Message>> sourceContext)
      throws Exception {
    long timestamp_us = Long.MIN_VALUE; // AKA current global watermark
    while (!isCancelled) {

      Tuple2<Integer, Message> srcNodeIdMessage = parsedMessageStream.take();
      timestamp_us =
          Math.max(
              LocalTime.now().toNanoOfDay() / 1000L,
              timestamp_us + 1); // ensures t_us is strictly increasing

      LOG.debug(
          "retrieved the message from parsedMessageStream: {}", srcNodeIdMessage.f1.toString());
      LOG.debug("message is of type {}", srcNodeIdMessage.f1.getClass());
      List<Class> eventClasses = Arrays.asList(SimpleEvent.class, ComplexEvent.class, Event.class);

      if (eventClasses.contains(srcNodeIdMessage.f1.getClass())) {

        Event event = (Event) srcNodeIdMessage.f1;
        Tuple2<Integer, Message> srcNodeIdEvent = new Tuple2<>(srcNodeIdMessage.f0, event);
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

        Long currentTime = TimeUtils.getCurrentTimeInMicroseconds();
        boolean beforeShiftTime =
            currentTime
                < shiftTimestamp.orElse(
                    0L); // 0L for when timestampts are not set yet and to avoid yet another if-else
        boolean isTransitionPhase =
            driftTimestamp.isPresent()
                && (!shiftTimestamp.isPresent() || (shiftTimestamp.isPresent() && beforeShiftTime));
        LOG.debug(
            "currentTime = {}; shiftTimestamp.orElse(0L) = {}; beforeShiftTime = {};"
                + " driftTimestamp.isPresent() = {}; shiftTimestamp.isPresent() = {};"
                + " isTransitionPhase = {}",
            currentTime,
            shiftTimestamp.orElse(0L),
            beforeShiftTime,
            driftTimestamp.isPresent(),
            shiftTimestamp.isPresent(),
            isTransitionPhase);

        if (isTransitionPhase) {
          // TODO: make sure the buffer is cleared for GC after the shift (it's cleared in the
          // timerTask tho)
          LOG.info(
              "rateMonitoringInputs.isNonFallbackNode(this.nodeId) = {}"
                  + config.rateMonitoringInputs.isNonFallbackNode(config.nodeId),
              srcNodeIdEvent.f1.toString());
          LOG.info(
              "event.eventType.equals(rateMonitoringInputs.partitioningInput) = {} for event '{}'",
              event.eventType.equals(config.rateMonitoringInputs.partitioningInput),
              event.toString());

          if (config.rateMonitoringInputs.isNonFallbackNode(config.nodeId)
              && event.eventType.equals(config.rateMonitoringInputs.partitioningInput)) {
            event.addAttribute("flushed");
            this.partEventBuffer.add(event);
            LOG.info("Inserted event {} into the nonPartBuffer", srcNodeIdEvent.f1);
          }
        }

      } else if (srcNodeIdMessage.f1.getClass().equals(ControlEvent.class)) {
        LOG.debug("message is of type ControlEvent");
        ControlEvent controlEvent = (ControlEvent) srcNodeIdMessage.f1;

        if (controlEvent.driftTimestamp.isPresent() && !driftTimestamp.isPresent()) {
          driftTimestamp = Optional.of(controlEvent.driftTimestamp.get());

          try {
            assert driftTimestamp.isPresent();
          } catch (AssertionError e) {
            LOG.error(
                "Setting driftTimestamp from control message {} failed with error: {}",
                controlEvent.toString(),
                e.getMessage());
            e.printStackTrace();
          }

          LOG.info("Drift timestamp: " + TimeUtils.format(driftTimestamp.get()));

          if (controlEvent.shiftTimestamp.isPresent() && !shiftTimestamp.isPresent()) {
            shiftTimestamp = Optional.of(controlEvent.shiftTimestamp.get());

            try {
              assert shiftTimestamp.isPresent();
            } catch (AssertionError e) {
              LOG.error(
                  "Setting shiftTimestamp from control message {} failed with error: {}",
                  controlEvent.toString(),
                  e.getMessage());
              e.printStackTrace();
            }

            Long transitionDurationInSec =
                getTimeDeltaInSec(shiftTimestamp.get(), driftTimestamp.get());
            LOG.info(
                "Drift timestamp: {}; Shift timestamp: {}; Transition duration: {} seconds",
                TimeUtils.format(driftTimestamp.get()),
                TimeUtils.format(shiftTimestamp.get()),
                transitionDurationInSec);

            try {
              ScheduledTask scheduledPartEventBufferFlush =
                  new ScheduledTask(
                      this.partEventBuffer,
                      this.config.forwarding.table,
                      this.config.forwarding.updatedTable,
                      this.config.rateMonitoringInputs,
                      this.config.nodeId,
                      this.config.forwarding.addressBookTCP,
                      multiSinkQueryEnabled);
              ScheduledExecutorService scheduledExecutorService =
                  Executors.newScheduledThreadPool(1);
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

            // src node ID is -1 for now since flink can't serialize null in Tuple2
            sourceContext.collectWithTimestamp(new Tuple2<>(-1, controlEvent), timestamp_us);
            sourceContext.emitWatermark(new Watermark(timestamp_us));
            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "From node "
                      + srcNodeIdMessage.f0
                      + ": "
                      + "Event "
                      + controlEvent.toString()
                      + " collected with timestamp "
                      + timestamp_us);
              LOG.trace("Watermark: " + timestamp_us);
            }
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
        if (message == null || message.contains("end-of-the-stream")) {
          LOG.info("Reached the end of the stream for " + client_address);
          if (message == null) {
            // do nothing, let the client reconnect after a one-time buffer flush
            // for regular event forwarding
            LOG.warn("Stream terminated without end-of-the-stream marker.");
            continue;
          }
          input.close();
          socket.close();
          return;

        } else if (message.startsWith("control")) {
          Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
          LOG.debug("Parsed controlEvent: {}", controlEvent);

          try {
            assert controlEvent.isPresent();
            assert controlEvent.get().getClass() == ControlEvent.class;
            assert controlEvent.get().driftTimestamp.isPresent();
          } catch (AssertionError e) {
            LOG.error("Parsing control event {} failed with {}", message, e.getMessage());
            e.printStackTrace();
          }

          parsedMessageStream.put(new Tuple2<>(null, controlEvent.get()));
          LOG.info("Inserted control event into the parsedMessageStream: {}", controlEvent.get());

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
          LOG.debug("multiSinkQueryEnabled flag: {}", multiSinkQueryEnabled);
          Event event = Event.parse(message);
          event.setMultiSinkQueryEnabled(multiSinkQueryEnabled.get());

          try {
            assert event.multiSinkQueryEnabled == multiSinkQueryEnabled.get();
          } catch (AssertionError e) {
            LOG.error(
                "multiSinkQueryEnabled flag mismatch for event {} and global flag"
                    + " multiSinkQueryEnabled {}",
                event.toString(),
                multiSinkQueryEnabled.get());
            e.printStackTrace();
          }
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
