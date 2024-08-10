// package monitor;
package com.huberlin.monitor;

import com.huberlin.event.ControlEvent;
import com.huberlin.sharedconfig.RateMonitoringInputs;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MonitoringData implements Runnable {
  final long timeWindow = 10;
  final long timeSlide = 1;
  long cutoffTimestamp;
  int steinerTreeSize;
  BlockingEventBuffer buffer;
  HashMap<String, Double> nonPartInputRates = new HashMap<>();
  HashMap<String, Double> partInputRates = new HashMap<>();
  HashMap<String, Double> matchRates = new HashMap<>();
  HashMap<String, Integer> nodesPerItem = new HashMap<>();
  HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates;
  int nodeId;
  int nodePort;
  int coordinatorPort;

  public MonitoringData(
      BlockingEventBuffer buffer,
      RateMonitoringInputs rateMonitoringInputs,
      HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates,
      int nodeId,
      int nodePort,
      int coordinatorPort) {
    this.totalRates = totalRates;
    this.cutoffTimestamp = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(timeWindow) - 1;
    this.buffer = buffer;
    int totalRatesCapacity = 600 * 120; // every second for 10 min
    for (String eventType : rateMonitoringInputs.nonPartitioningInputs) {
      this.nonPartInputRates.put(eventType, 0.0);
      this.nodesPerItem.put(eventType, rateMonitoringInputs.numNodesPerQueryInput.get(eventType));
      this.totalRates.put(eventType, new ArrayBlockingQueue<>(totalRatesCapacity));
    }
    this.partInputRates.put(rateMonitoringInputs.partitioningInput, 0.0);
    totalRates.put(
        rateMonitoringInputs.partitioningInput, new ArrayBlockingQueue<>(totalRatesCapacity));
    this.nodesPerItem.put(
        rateMonitoringInputs.partitioningInput,
        rateMonitoringInputs.numNodesPerQueryInput.get(rateMonitoringInputs.partitioningInput));
    this.matchRates.put(rateMonitoringInputs.multiSinkQuery, 0.0);
    totalRates.put(
        rateMonitoringInputs.multiSinkQuery, new ArrayBlockingQueue<>(totalRatesCapacity));
    this.nodesPerItem.put(
        rateMonitoringInputs.multiSinkQuery, rateMonitoringInputs.multiSinkNodes.size());
    this.steinerTreeSize = rateMonitoringInputs.steinerTreeSize;

    this.nodesPerItem.forEach(
        (k, v) -> {
          System.out.println("Item: " + k + " Nodes: " + v);
        });
    this.nodeId = nodeId;
    this.nodePort = nodePort;
    this.coordinatorPort = coordinatorPort;
  }

  public long getCurrentTimeInSeconds() {
    // Get the current time
    LocalTime now = LocalTime.now();

    // Calculate the total seconds since the start of the day
    long hoursInSeconds = now.getHour() * 60L * 60L;
    long minutesInSeconds = now.getMinute() * 60L;
    long seconds = now.getSecond();

    return hoursInSeconds + minutesInSeconds + seconds;
  }

  public Double updateRates(BlockingEventBuffer buffer, String eventType) {
    this.cutoffTimestamp =
        TimeUtils.getCurrentTimeInMicroseconds() - TimeUnit.SECONDS.toMicros(timeWindow) - 1;
    buffer.clearOutdatedEvents(this.cutoffTimestamp);
    Integer numEvents =
        buffer.stream()
            .filter(e -> e.getEventType().equals(eventType))
            .collect(Collectors.toList())
            .size();
    Double rate = Double.valueOf(numEvents) / Double.valueOf(timeWindow);
    Double totalRate = rate * nodesPerItem.get(eventType);

    this.totalRates.get(eventType).add(new TimestampAndRate(getCurrentTimeInSeconds(), totalRate));
    return totalRate;
  }

  public void updateAllRates() {
    System.out.println("\nBuffer size before dropping old events: " + this.buffer.size());
    System.out.println(this.buffer.toString());
    System.out.println(
        "Dropping events with timestamp <= " + TimeUtils.format(this.cutoffTimestamp));
    System.out.println("Buffer size before dropping old events: " + this.buffer.size());
    for (String eventType : nonPartInputRates.keySet()) {
      Double newRate = updateRates(buffer, eventType);
      nonPartInputRates.put(eventType, newRate);
    }
    for (String eventType : partInputRates.keySet()) {
      Double newRate = updateRates(buffer, eventType);
      partInputRates.put(eventType, newRate);
    }
    for (String eventType : matchRates.keySet()) {
      Double newRate = updateRates(buffer, eventType);
      matchRates.put(eventType, newRate);
    }
    System.out.println("Buffer size after dropping old events: " + this.buffer.size() + "\n");
    System.out.println(this.buffer.toString());
  }

  public boolean inequalityHolds() {
    Double partInputRate = (Double) partInputRates.values().toArray()[0];
    Double totalPartInputRate =
        partInputRate * nodesPerItem.get(partInputRates.keySet().toArray()[0]);
    Double matchRate = (Double) matchRates.values().toArray()[0];
    Double totalMatchRate = matchRate * nodesPerItem.get(matchRates.keySet().toArray()[0]);
    Double totalNonPartInputRate = 0.0;
    for (String eventType : nonPartInputRates.keySet()) {
      totalNonPartInputRate += nonPartInputRates.get(eventType) * nodesPerItem.get(eventType);
    }
    Double totalRhs = totalNonPartInputRate * this.steinerTreeSize + totalMatchRate;
    if (totalPartInputRate < totalRhs) {
      System.out.println("Partitioning input rate is too low for the multi-sink placement");
      String msg =
          "Partitioning input rate: "
              + totalPartInputRate
              + " < "
              + "RHS: "
              + totalRhs
              + " = "
              + totalNonPartInputRate
              + " * "
              + this.steinerTreeSize
              + " + "
              + totalMatchRate;
      return false;
      // System.out.println("Partitioning input rate: " + totalPartInputRate);
      // System.out.println("Match rate: " + totalMatchRate);
      // System.out.println("Non-partitioning input rate: " + totalNonPartInputRate);
    }
    return true;
  }

  public void printRates() {
    System.out.println("Non-partitioning input rates:");
    for (String eventType : nonPartInputRates.keySet()) {
      System.out.println(
          eventType
              + ": "
              + nonPartInputRates.get(eventType)
              + " * "
              + nodesPerItem.get(eventType)
              + " = "
              + nonPartInputRates.get(eventType) * nodesPerItem.get(eventType));
    }
    System.out.println("Partitioning input rates:");
    for (String eventType : partInputRates.keySet()) {
      System.out.println(
          eventType
              + ": "
              + partInputRates.get(eventType)
              + " * "
              + nodesPerItem.get(eventType)
              + " = "
              + partInputRates.get(eventType) * nodesPerItem.get(eventType));
    }
    System.out.println("Match rates:");
    for (String eventType : matchRates.keySet()) {
      System.out.println(
          eventType
              + ": "
              + matchRates.get(eventType)
              + " * "
              + nodesPerItem.get(eventType)
              + " = "
              + matchRates.get(eventType) * nodesPerItem.get(eventType));
    }
  }

  // TODO: wrap into a thread
  public void sendControlEvent(ControlEvent controlEvent, int port) {
    try {
      Socket socket = new Socket("localhost", port);
      PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
      writer.println("I am " + this.nodeId + " monitor");
      writer.println(controlEvent.toString());
      writer.println("end-of-the-stream\n");
      writer.close();
      socket.close();
      System.out.println("Sent control event: " + controlEvent.toString() + " to port " + port);
    } catch (IOException e) {
      System.out.println(
          "Failure to establish connection from monitor to port "
              + port
              + " for control event. Error: "
              + e);
      e.printStackTrace();
    }
  }

  public void run() {
    System.out.println("Monitoring thread started. Cuttoff timestamp: " + cutoffTimestamp);
    int inequalityViolationsInARow = 0;
    while (true) {
      System.out.println("===========\n New rates...\n===========");
      updateAllRates();
      printRates();
      if (!inequalityHolds()) {
        if (inequalityViolationsInARow >= 3) {
          System.out.println("=========== TRIGGER SWITCH ===========");
          long t = TimeUtils.getCurrentTimeInMicroseconds();
          System.out.println("driftTimestamp = " + t);
          ControlEvent controlEvent = new ControlEvent(Optional.of(t), Optional.empty());
          sendControlEvent(controlEvent, nodePort);
          sendControlEvent(controlEvent, coordinatorPort);
          // break;
        }
        inequalityViolationsInARow++;
        System.out.println(
            "Inequality does not hold. Violations in a row: " + inequalityViolationsInARow);
      } else {
        inequalityViolationsInARow = 0;
      }
      System.out.println("====================================");
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1)); // TODO: use timeSlide in real prog
      } catch (InterruptedException e) {
        System.out.println("Monitoring thread interrupted");
        e.printStackTrace();
      }
    }
  }
}
