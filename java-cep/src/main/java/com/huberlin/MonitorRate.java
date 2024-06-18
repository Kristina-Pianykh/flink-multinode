package com.huberlin;

import com.huberlin.config.NodeConfig;
import com.huberlin.event.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

public class MonitorRate extends RichMapFunction<Event, Tuple2<Double, Long>> {
  public transient Meter inputEventRateMeter;
  public String name;
  public NodeConfig.RateMonitoringInputs inputs;
  private int interval = 10;

  public MonitorRate(String name, NodeConfig.RateMonitoringInputs inputs) {
    this.name = name;
    this.inputs = inputs;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // System.out.println("MonitorInputRate open");
    inputEventRateMeter =
        getRuntimeContext().getMetricGroup().meter(this.name, new MeterView(interval));
  }

  @Override
  public Tuple2<Double, Long> map(Event event) {
    Double globalRate;
    inputEventRateMeter.markEvent();
    System.out.println(
        this.name + ": " + inputEventRateMeter.getRate() + " per " + this.interval + " seconds\n");
    long timestamp = System.currentTimeMillis();
    // FIXME: silent errors block the job without any error message
    if (!event.isSimple()) {
      globalRate =
          inputs.multiSinkNodes.stream().mapToInt(Integer::intValue).sum()
              * inputEventRateMeter.getRate();
    } else {
      globalRate =
          inputs.numNodesPerQueryInput.get(event.getEventType()) * inputEventRateMeter.getRate();
    }
    return new Tuple2<>(globalRate, timestamp);
  }
}
