package com.huberlin.monitor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.cli.*;
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

  private static CommandLine parse_cmdline_args(String[] args) {
    final Options cmdline_opts = new Options();
    final HelpFormatter formatter = new HelpFormatter();
    // cmdline_opts.addOption(new Option("node", true, "Node ID on which the monitor is running"));
    cmdline_opts.addOption(new Option("applyStrategy", false, "test"));
    final CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(cmdline_opts, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("java -jar cep-node.jar", cmdline_opts);
      System.exit(1);
    }
    return null;
  }

  // @Test
  // void cliOptionTest() {
  //   // String[] args = new String[] {"-node", "1"};
  //   CommandLine cmd = parse_cmdline_args(args);
  //   // System.out.println("node: " + cmd.getOptionValue("node"));
  //
  //   if (cmd.hasOption("applyStrategy")) {
  //     System.out.println("applyStrategy set");
  //   } else {
  //     System.out.println("applyStrategy: null");
  //   }
  // }
}
