package com.huberlin.monitor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import org.junit.jupiter.api.Test;

class GeneralTest {

  @Test
  void FileWriterTest() {
    HashMap<String, ArrayBlockingQueue<TimestampAndRate>> totalRates;
    totalRates = new HashMap<>();

    String filePath = "testitest";
    new Thread(new FileWrite(0, totalRates, filePath)).start();

    totalRates.put("popo", new ArrayBlockingQueue<>(10));
    totalRates.get("popo").add(new TimestampAndRate(1, 1.0));
    for (String key : totalRates.keySet()) {
      System.out.println(key + " : " + totalRates.get(key).toString());
    }
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    File f = new File(filePath);
    assert f.exists();

    totalRates.put("pepe", new ArrayBlockingQueue<>(10));
    totalRates.get("pepe").add(new TimestampAndRate(2, 2.0));
    for (String key : totalRates.keySet()) {
      System.out.println(key + " : " + totalRates.get(key).toString());
    }
    assert totalRates.size() == 2;
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
