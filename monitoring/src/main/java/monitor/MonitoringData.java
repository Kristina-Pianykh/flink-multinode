package monitor;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MonitoringData implements Runnable {
  final long timeWindow = 10;
  final long timeSlide = 1;
  long cutoffTimestamp;
  BlockingEventBuffer buffer;
  HashMap<String, Double> nonPartInputRates = new HashMap<>();
  HashMap<String, Double> partInputRates = new HashMap<>();
  HashMap<String, Double> matchRates = new HashMap<>();
  HashMap<String, Integer> nodesPerItem = new HashMap<>();
  int steinerTreeSize;

  public MonitoringData(BlockingEventBuffer buffer, RateMonitoringInputs rateMonitoringInputs) {
    this.cutoffTimestamp = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(timeWindow) - 1;
    this.buffer = buffer;
    for (String eventType : rateMonitoringInputs.nonPartitioningInputs) {
      this.nonPartInputRates.put(eventType, 0.0);
      this.nodesPerItem.put(eventType, rateMonitoringInputs.numNodesPerQueryInput.get(eventType));
    }
    this.partInputRates.put(rateMonitoringInputs.partitioningInput, 0.0);
    this.nodesPerItem.put(
        rateMonitoringInputs.partitioningInput,
        rateMonitoringInputs.numNodesPerQueryInput.get(rateMonitoringInputs.partitioningInput));
    this.matchRates.put(rateMonitoringInputs.multiSinkQuery, 0.0);
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
    return totalRate;
  }

  public void updateAllRates() {
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
  }

  public void compareTotalRates() {
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
      System.out.println("Partitioning input rate: " + totalPartInputRate);
      System.out.println("Match rate: " + totalMatchRate);
      System.out.println("Non-partitioning input rate: " + totalNonPartInputRate);
    }
  }

  public void printRates() {
    System.out.println("Non-partitioning input rates:");
    for (String eventType : nonPartInputRates.keySet()) {
      System.out.println(eventType + ": " + nonPartInputRates.get(eventType));
    }
    System.out.println("Partitioning input rates:");
    for (String eventType : partInputRates.keySet()) {
      System.out.println(eventType + ": " + partInputRates.get(eventType));
    }
    System.out.println("Match rates:");
    for (String eventType : matchRates.keySet()) {
      System.out.println(eventType + ": " + matchRates.get(eventType));
    }
  }

  public void run() {
    System.out.println("Monitoring thread started. Cuttoff timestamp: " + cutoffTimestamp);
    while (true) {
      System.out.println("===========\n New rates...\n===========");
      updateAllRates();
      printRates();
      compareTotalRates();
      System.out.println("====================================");
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1)); // TODO: use timeSlide in real prog
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
