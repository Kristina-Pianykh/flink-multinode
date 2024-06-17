package com.huberlin;

import com.huberlin.event.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

public class MonitorMatchRate extends RichMapFunction<Event, Tuple2<Double, Long>> {
  public transient Meter matchRateMeter;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // System.out.println("MonitorMatchRate initialized");
    matchRateMeter = getRuntimeContext().getMetricGroup().meter("matchRate", new MeterView(10));
  }

  @Override
  public Tuple2<Double, Long> map(Event event) {
    matchRateMeter.markEvent();
    System.out.println(matchRateMeter.getRate() + " match events per second\n");
    long timestamp = System.currentTimeMillis();
    return new Tuple2<>(matchRateMeter.getRate(), timestamp);
  }
}
