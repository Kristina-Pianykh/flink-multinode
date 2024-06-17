package com.huberlin;

import com.huberlin.config.NodeConfig;
import com.huberlin.event.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

public class MonitorRate
    extends RichMapFunction<Tuple2<String, Event>, Tuple3<String, Double, Long>> {
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
  public Tuple3<String, Double, Long> map(Tuple2<String, Event> item) {
    Event event = item.f1;
    inputEventRateMeter.markEvent();
    System.out.println(
        this.name + ": " + inputEventRateMeter.getRate() + " per " + this.interval + "secodns.\n");
    long timestamp = System.currentTimeMillis();
    Double globalRate =
        inputs.numNodesPerQueryInput.get(event.getEventType()) * inputEventRateMeter.getRate();
    return new Tuple3<>(event.getEventType(), inputEventRateMeter.getRate(), timestamp);
  }
}
