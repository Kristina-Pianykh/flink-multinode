package monitor;

import java.time.LocalTime;
import java.util.HashMap;
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
  HashMap<String, ArrayBlockingQueue<Double>> totalRates;

  public MonitoringData(
      BlockingEventBuffer buffer,
      RateMonitoringInputs rateMonitoringInputs,
      HashMap<String, ArrayBlockingQueue<Double>> totalRates) {
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
  }

  public long getCurrentTimeInMicroseconds() {
    // Get the current time
    LocalTime now = LocalTime.now();

    // Calculate the total microseconds since the start of the day
    long hoursInMicroseconds = now.getHour() * 60L * 60L * 1_000_000L;
    long minutesInMicroseconds = now.getMinute() * 60L * 1_000_000L;
    long secondsInMicroseconds = now.getSecond() * 1_000_000L;
    long nanoInMicroseconds = now.getNano() / 1_000L;

    return hoursInMicroseconds + minutesInMicroseconds + secondsInMicroseconds + nanoInMicroseconds;
  }

  public Double updateRates(BlockingEventBuffer buffer, String eventType) {
    this.cutoffTimestamp =
        getCurrentTimeInMicroseconds() - TimeUnit.SECONDS.toMicros(timeWindow) - 1;
    buffer.clearOutdatedEvents(this.cutoffTimestamp);
    Integer numEvents =
        buffer.stream()
            .filter(e -> e.getEventType().equals(eventType))
            .collect(Collectors.toList())
            .size();
    Double rate = Double.valueOf(numEvents) / Double.valueOf(timeWindow);
    Double totalRate = rate * nodesPerItem.get(eventType);
    this.totalRates.get(eventType).add(totalRate);
    return totalRate;
  }

  public void updateAllRates() {
    System.out.println("\nBuffer size before dropping old events: " + this.buffer.size());
    System.out.println(this.buffer.toString());
    System.out.println(
        "Dropping events with timestamp <= " + FormatTimestamp.format(this.cutoffTimestamp));
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
          break;
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
        e.printStackTrace();
      }
    }
  }
}
