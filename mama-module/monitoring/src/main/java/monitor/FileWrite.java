package com.huberlin.monitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWrite extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(FileWrite.class);
  private volatile boolean running = true;
  private Integer nodeId;
  private HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates;
  private final String filePath;
  private final int writeIntervalMilis = 30000; // 30 seconds

  public FileWrite(
      Integer nodeId, HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates) {
    this.nodeId = nodeId;
    this.totalRates = totalRates;
    this.filePath = "totalRates" + this.nodeId + ".csv";

    File f = new File(filePath);
    if (!f.exists()) {
      try {
        f.createNewFile();
        assert f.exists();
      } catch (IOException e) {
        LOG.error("Error creating file: {}", e.getMessage());
        System.exit(1);
      } catch (AssertionError e) {
        LOG.error("Error creating file: {}", e.getMessage());
        System.exit(1);
      }
    }
    LOG.info("filePath: {} " + this.filePath);
  }

  public FileWrite(
      Integer nodeId,
      HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates,
      String path) {
    this.nodeId = nodeId;
    this.totalRates = totalRates;
    this.filePath = path;

    File f = new File(filePath);
    if (!f.exists()) {
      try {
        f.createNewFile();
        assert f.exists();
      } catch (IOException e) {
        LOG.error("Error creating file: {}", e.getMessage());
        System.exit(1);
      } catch (AssertionError e) {
        LOG.error("Error creating file: {}", e.getMessage());
        System.exit(1);
      }
    }
    LOG.info("filePath: {} " + this.filePath);
    LOG.info("filePath: {} " + this.filePath);
  }

  public void terminate() {
    running = false;
  }

  @Override
  public void run() {
    try {
      while (running) {
        Thread.sleep(writeIntervalMilis);
        writeHashMapToCSV();
      }
    } catch (InterruptedException e) {
      LOG.warn(
          "Thread interrupted with {}. Writing the last batch of dat to disk...", e.getMessage());
      writeHashMapToCSV();
      System.exit(1);
    }
    writeHashMapToCSV();
    System.exit(1);
  }

  public void writeHashMapToCSV() {
    try {
      PrintWriter writer =
          new PrintWriter(
              new BufferedWriter(
                  new OutputStreamWriter(new FileOutputStream(this.filePath, false), "utf-8")));

      try {
        assert writer != null;
      } catch (NullPointerException e) {
        LOG.error("Error creating writer: {}", e.getMessage());
        System.exit(1);
      }

      try {
        writer.write("EventType,Rate\n");
        StringBuilder sb = new StringBuilder();

        // Write the data
        for (Map.Entry<String, ArrayBlockingQueue<TimestampAndRate>> entry :
            this.totalRates.entrySet()) {
          String eventType = entry.getKey();
          ArrayBlockingQueue<TimestampAndRate> timestampsAndRates = entry.getValue();
          for (TimestampAndRate timestampAndRate : timestampsAndRates) {
            sb.append("\""); // Wrap the eventType in quotes (in case it contains a comma)
            sb.append(eventType);
            sb.append("\"");
            sb.append(",");
            sb.append(timestampAndRate.timestamp);
            sb.append(",");
            sb.append(timestampAndRate.rate);
            sb.append("\n");
          }
        }

        writer.write(sb.toString());
        writer.flush();
        writer.close();
        LOG.info("Wrote data to file: " + this.filePath);
      } catch (Exception e) {
        LOG.error("Error writing to file: {}", e.getMessage());
      }

    } catch (IOException e) {
      LOG.error("Error writing to file: {}", e.getMessage());
    }
  }
}
