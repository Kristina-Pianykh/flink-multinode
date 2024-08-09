package com.huberlin.javacep;

import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.javacep.communication.OldSourceFunction;
import com.huberlin.javacep.communication.SendToMonitor;
import com.huberlin.javacep.communication.TCPEventSender;
import com.huberlin.javacep.config.NodeConfig;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

  private static CommandLine parse_cmdline_args(String[] args) {
    final Options cmdline_opts = new Options();
    final HelpFormatter formatter = new HelpFormatter();
    cmdline_opts.addOption(new Option("localconfig", true, "Path to the local configuration file"));
    cmdline_opts.addOption(
        new Option("globalconfig", true, "Path to the global configuration file"));
    cmdline_opts.addOption(
        new Option("flinkconfig", true, "Path to the directory with the flink configuration"));
    cmdline_opts.addOption(
        new Option(
            "monitoringinputs",
            true,
            "Path to the directory with the inputs for computing the inequality"));
    cmdline_opts.addOption(
        new Option("updatedrules", true, "Path to the file with the updated forwarding rules"));
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

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parse_cmdline_args(args);

    String filePath_local = cmd.getOptionValue("localconfig", "./conf/config.json");
    String filePath_global = cmd.getOptionValue("globalconfig", "./conf/address_book.json");
    String rateMonitoringInputsPath = cmd.getOptionValue("monitoringinputs", null);
    String updatedForwardingRulesPath = cmd.getOptionValue("updatedrules", null);
    assert rateMonitoringInputsPath != null : "Path for rate monitoring inputs is not set";
    assert updatedForwardingRulesPath != null
        : "Path for the file with the updated forwarding rules is not set";

    NodeConfig config = new NodeConfig();
    config.parseJsonFile(
        filePath_local, filePath_global, rateMonitoringInputsPath, updatedForwardingRulesPath);
    if (config != null) {
      System.out.println("Parsed JSON successfully");
    } else {
      LOG.error("Failed to parse JSON");
      System.exit(1);
    }

    String partInput = config.rateMonitoringInputs.partitioningInput;
    System.out.println(
        "Destinations for partitioning input in the updated forwarding table: "
            + "\n    "
            + config.forwarding.updatedTable.lookupUpdated(partInput));

    final int REST_PORT = 8081 + config.nodeId * 2;
    Configuration flinkConfig =
        GlobalConfiguration.loadConfiguration(cmd.getOptionValue("flinkconfig", "conf"));
    flinkConfig.set(JobManagerOptions.RPC_BIND_PORT, 6123 + config.nodeId);
    flinkConfig.set(JobManagerOptions.PORT, 6123 + config.nodeId);
    flinkConfig.set(RestOptions.BIND_PORT, REST_PORT + "-" + (REST_PORT + 1));
    flinkConfig.set(RestOptions.PORT, REST_PORT);
    flinkConfig.set(TaskManagerOptions.RPC_BIND_PORT, 51000 + config.nodeId);
    flinkConfig.set(TaskManagerOptions.RPC_PORT, "0");
    flinkConfig.set(BlobServerOptions.PORT, "0");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    FileSystem.initialize(flinkConfig);

    // Only one engine-thread will work (output is displayed in the same way the packets arrive)
    env.setParallelism(1);

    DataStream<Tuple2<Integer, Event>> inputStream = env.addSource(new OldSourceFunction(config));

    /* for fallback node only: filter out the non-partitioning inputs for
    to apply a pattern with 2x window size for retrospective matching
    with the fluahed partitioning inputs (once only) */
    if (config.nodeId == config.rateMonitoringInputs.fallbackNode) {
      LOG.debug("I am the fallback node");
      DataStream<Tuple2<Integer, Event>> nonPartitioningInputStream =
          inputStream.filter(
              new FilterFunction<Tuple2<Integer, Event>>() {

                @Override
                public boolean filter(Tuple2<Integer, Event> srcIdEvent) {
                  Event event = srcIdEvent.f1;
                  LOG.debug(
                      "Event {} is non-partitioning: {}",
                      event.getEventType(),
                      config.rateMonitoringInputs.nonPartitioningInputs.contains(
                          event.getEventType()));
                  if (config.rateMonitoringInputs.nonPartitioningInputs.contains(
                      event.getEventType())) {
                    LOG.debug("Filtered out a non-partitioning input: {}", event.toString());
                    return true;
                  }
                  return false;
                }
              });

      DataStream<Tuple2<Integer, Event>> flushedPartitioningInputsStream =
          inputStream.filter(
              new FilterFunction<Tuple2<Integer, Event>>() {

                @Override
                public boolean filter(Tuple2<Integer, Event> srcIdEvent) {
                  Event event = srcIdEvent.f1;
                  boolean isPartitioningInput =
                      config.rateMonitoringInputs.partitioningInput.equals(event.getEventType());
                  boolean isFlushedEvent = event.isFlushed();
                  if (isFlushedEvent) {
                    LOG.debug("Detected flushed event: {}", event);
                    LOG.debug("isPartitioningInput = {}", isPartitioningInput);
                    LOG.debug("isFlushedEvent = {}", isFlushedEvent);
                  }
                  if (isPartitioningInput && isFlushedEvent) return true;
                  else return false;
                }
              });

      DataStream<Tuple2<Integer, Event>> shiftMultiSinkQueryInputs =
          nonPartitioningInputStream.union(flushedPartitioningInputsStream);

      /* DEBUGGING STEP */
      List<NodeConfig.Processing> queries =
          config.processing.stream()
              .filter(q -> q.queryName.equals(config.rateMonitoringInputs.multiSinkQuery))
              .collect(Collectors.toList());
      try {
        assert queries.size() == 1;
        assert queries.get(0).queryName.equals(config.rateMonitoringInputs.multiSinkQuery);
        LOG.debug(
            "Found multi-sink query {} on the fallback node {}",
            queries.get(0).queryName,
            config.nodeId);
      } catch (AssertionError e) {
        LOG.error("Assertions about queries failed with: {}", e.getMessage());
        System.exit(1);
      }
      /* DEBUGGING STEP */

      DataStream<Event> matchedMultiSinkQueryOutput =
          PatternFactory_generic.processQuery(
              queries.get(0), config, shiftMultiSinkQueryInputs.map((tuple) -> tuple.f1), 2);

      matchedMultiSinkQueryOutput.filter(
          new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) {
              LOG.debug("Event matched retrospectively: {}", event);
              return true;
            }
          });
    }

    // important check if node is one of the multi-sink nodes
    if (config.rateMonitoringInputs.multiSinkNodes.contains(config.nodeId)) {
      DataStream<Event> eventsForMonitoring = inputStream.map((tuple) -> tuple.f1);
      eventsForMonitoring.addSink(
          new SendToMonitor(
              config.nodeId,
              config.hostAddress.port,
              config.rateMonitoringInputs.multiSinkNodes.contains(config.nodeId)));
    }

    // TODO: remove this block?
    SingleOutputStreamOperator<Tuple2<Integer, Event>> monitored_stream =
        inputStream
            .map(
                new RichMapFunction<Tuple2<Integer, Event>, Tuple2<Integer, Event>>() {
                  private transient MetricsRecorder.MetricsWriter memory_usage_writer;
                  private transient MetricsRecorder.MemoryUsageRecorder memory_usage_recorder;
                  private transient Thread memory_usage_recorder_thread;

                  @Override
                  public void open(Configuration parameters) {
                    // Initialize MetricsWriter
                    memory_usage_writer =
                        new MetricsRecorder.MetricsWriter(
                            "memory_usage_node_" + config.nodeId + ".csv");

                    // Initialize and start the memory usage recorder thread
                    memory_usage_recorder =
                        new MetricsRecorder.MemoryUsageRecorder(memory_usage_writer);

                    memory_usage_recorder_thread = new Thread(memory_usage_recorder);

                    memory_usage_recorder_thread.start();
                  }

                  @Override
                  public Tuple2<Integer, Event> map(
                      Tuple2<Integer, Event> event_with_source_node_id) {
                    return event_with_source_node_id;
                  }

                  @Override
                  public void close() {
                    // Stop and clean up the recorder threads and MetricsWriter
                    memory_usage_recorder_thread.interrupt();

                    try {
                      memory_usage_recorder_thread.join();
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }
                })
            .uid("metrics");

    List<DataStream<Event>> outputstreams_by_query =
        PatternFactory_generic.processQueries(
            config.processing, config, inputStream.map((tuple) -> tuple.f1), 1);

    if (!outputstreams_by_query.isEmpty()) {
      outputstreams_by_query.forEach(
          stream ->
              stream.addSink(
                  new SendToMonitor(
                      config.nodeId,
                      config.hostAddress.port,
                      config.rateMonitoringInputs.multiSinkNodes.contains(
                          config.nodeId)))); // for debugging now it's only node 1
    }

    DataStream<Tuple2<Integer, Event>> outputStream;
    if (outputstreams_by_query.isEmpty()) outputStream = inputStream;
    else {
      DataStream<Event> union =
          outputstreams_by_query.stream().reduce(DataStream<Event>::union).get();
      outputStream =
          union
              .map(
                  new MapFunction<Event, Tuple2<Integer, Event>>() {
                    @Override
                    public Tuple2<Integer, Event> map(Event e) {
                      return new Tuple2<Integer, Event>(config.nodeId, e);
                    }
                  })
              .union(
                  inputStream); // FIXME: this will create duplicate tuples with the same event, if
      // union contains any of the events in inputStream

      // TODO: create sink that asserts that no event in inputstream.map(t -> t.f1) is in union
      // (pattery factory consumes *all* input events)

      // TODO:

    }

    DataStream<Tuple2<Integer, Event>> filteredOutputStream =
        outputStream.filter(
            new FilterFunction<Tuple2<Integer, Event>>() {
              @Override
              public boolean filter(Tuple2<Integer, Event> event_with_source_id) {
                Event e = event_with_source_id.f1;
                int source_node_id = event_with_source_id.f0;
                if (source_node_id == config.nodeId && !e.isSimple()) {
                  assert (e instanceof ComplexEvent);
                  ComplexEvent ce = (ComplexEvent) e;
                  System.out.println("LATENCYYYYYYYYYYYYYYYYYYYY " + (long) ce.getLatencyMs());
                }
                return true;
              }
            });

    filteredOutputStream.addSink(
        new TCPEventSender(
            config.forwarding.addressBookTCP,
            config.forwarding.table,
            config
                .nodeId)); // The cast expresses the fact that a TCPEventSender is a SinkFunction<?
    // extends Event>, not just a SInkFunction<Event>. I can't specify it in
    // java though.

    // Start cluster/CEP-engine
    env.execute("Flink Java API Skeleton");
  }
}
