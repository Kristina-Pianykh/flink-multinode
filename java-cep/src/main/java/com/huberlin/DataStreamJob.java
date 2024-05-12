/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huberlin;

import com.huberlin.communication.OldSourceFunction;
import com.huberlin.communication.TCPEventSender;
// import com.huberlin.config.JSONQueryParser;
import com.huberlin.config.NodeConfig;
import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import java.util.List;
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
  private static final Logger log = LoggerFactory.getLogger(DataStreamJob.class);

  private static CommandLine parse_cmdline_args(String[] args) {
    final Options cmdline_opts = new Options();
    final HelpFormatter formatter = new HelpFormatter();
    cmdline_opts.addOption(new Option("localconfig", true, "Path to the local configuration file"));
    cmdline_opts.addOption(
        new Option("globalconfig", true, "Path to the global configuration file"));
    cmdline_opts.addOption(
        new Option("flinkconfig", true, "Path to the directory with the flink configuration"));
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
    // Parse command line arguments

    CommandLine cmd = parse_cmdline_args(args);

    // Read global and local configuration files, and create config object ('QueryInformation')
    String filePath_local = cmd.getOptionValue("localconfig", "./conf/config.json"); // local config
    String filePath_global =
        cmd.getOptionValue("globalconfig", "./conf/address_book.json"); // global config
    NodeConfig config = new NodeConfig();
    config.parseJsonFile(filePath_local, filePath_global);
    //    NodeConfig config = JSONQueryParser.parseJsonFile(filePath_local, filePath_global);
    if (config != null) {
      System.out.println("Parsed JSON successfully");
      // You can now access the data structure's attributes, e.g., data.forwarding.send_mode or
      // data.processing.selectivity
    } else {
      log.error("Failed to parse JSON");
      System.exit(1);
    }
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

    // Set up flink;
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    FileSystem.initialize(flinkConfig);

    // Only one engine-thread will work (output is displayed in the same way the packets arrive)
    env.setParallelism(1);

    // find out if outputselection is used, if yes, change the ids of the events that are selected
    // for the match

    DataStream<Tuple2<Integer, Event>> inputStream =
        env.addSource(new OldSourceFunction(config.hostAddress.port));

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
            config.processing,
            inputStream.map((tuple) -> tuple.f1)); // input stream w/o source information

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
            config.forwarding.addressBook,
            config.forwarding.table,
            config
                .nodeId)); // The cast expresses the fact that a TCPEventSender is a SinkFunction<?
    // extends Event>, not just a SInkFunction<Event>. I can't specify it in
    // java though.

    // Start cluster/CEP-engine
    env.execute("Flink Java API Skeleton");
  }
}
