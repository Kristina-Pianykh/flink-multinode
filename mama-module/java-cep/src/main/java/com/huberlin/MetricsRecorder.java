package com.huberlin.javacep;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.flink.metrics.Meter;

public class MetricsRecorder {

  public static class MetricsWriter {
    private final String file_path;

    public MetricsWriter(String file_path) {
      this.file_path = file_path;
    }

    public synchronized void write_record(double value) throws IOException {
      try (PrintWriter writer = new PrintWriter(new FileWriter(file_path, true))) {
        writer.println(value);
      }
    }
  }

  public static class ThroughputRecorder implements Runnable {
    private final MetricsWriter throughput_writer;
    private final Meter throughput_meter;

    public ThroughputRecorder(MetricsWriter throughput_writer, Meter throughput_meter) {
      this.throughput_writer = throughput_writer;
      this.throughput_meter = throughput_meter;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          long current_throughput = throughput_meter.getCount();
          throughput_writer.write_record((double) current_throughput);
          throughput_meter.markEvent(-current_throughput);
          Thread.sleep(10000);
        }
      } catch (InterruptedException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static class MemoryUsageRecorder implements Runnable {
    private final MetricsWriter memory_usage_writer;

    public MemoryUsageRecorder(MetricsWriter memory_usage_writer) {
      this.memory_usage_writer = memory_usage_writer;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          long memory_usage =
              Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
          memory_usage_writer.write_record((double) memory_usage / 1000000);
          Thread.sleep(1000);
        }
      } catch (InterruptedException | IOException e) {
        e.printStackTrace();
      }
    }
  }
}
