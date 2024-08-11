package com.huberlin.javacep;

import com.huberlin.event.Event;
import java.time.LocalTime;
import org.apache.flink.api.common.eventtime.*;

public class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

  @Override
  public WatermarkGenerator<Event> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {
    return new CustomWatermarkGenerator();
  }

  @Override
  public TimestampAssigner<Event> createTimestampAssigner(
      TimestampAssignerSupplier.Context context) {
    return new CustomTimestampAssigner(); // extract timestamp from the event
  }

  public static class CustomTimestampAssigner implements TimestampAssigner<Event> {
    @Override
    public long extractTimestamp(Event element, long recordTimestamp) {
      return System.currentTimeMillis();
    }
  }

  public static class CustomWatermarkGenerator implements WatermarkGenerator<Event> {
    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
      long timestamp_us = LocalTime.now().toNanoOfDay() / 1000L;
      long watermark_ = System.currentTimeMillis();
      // System.out.println("Emitting on event watermark: " +
      // TimeUtils.format(Event.getTimestamp()));
      output.emitWatermark(new Watermark(watermark_));
      // output.emitWatermark(new Watermark(timestamp_us));
      //      output.emitWatermark(new Watermark(eventTimestamp));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
  }
}
