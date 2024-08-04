package com.huberlin.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
  private static final Logger LOG = LoggerFactory.getLogger(Test.class);
  private static final Random RANDOM = new Random();

  public static HashMap<String, HashMap<Integer, TreeSet<Integer>>> generateRandomTable(
      int eventTypeCount, int sourceNodeCount, int maxDestinationNodeCount) {
    HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();

    for (int i = 0; i < eventTypeCount; i++) {
      if (RANDOM.nextBoolean()) continue;
      String eventType = "EventType" + i;
      HashMap<Integer, TreeSet<Integer>> sourceNodeMap = new HashMap<>();

      for (int j = 0; j < sourceNodeCount; j++) {
        if (RANDOM.nextBoolean()) continue;
        Integer sourceNodeId = RANDOM.nextBoolean() ? null : RANDOM.nextInt(5);
        if (sourceNodeId != null && sourceNodeMap.containsKey(sourceNodeId)) {
          continue; // Avoid duplicate keys
        }
        TreeSet<Integer> destinationNodeIds = new TreeSet<>();

        int destinationNodeCount =
            RANDOM.nextInt(maxDestinationNodeCount) + 1; // At least one destination node
        for (int k = 0; k < destinationNodeCount; k++) {
          if (RANDOM.nextBoolean()) continue;
          destinationNodeIds.add(
              RANDOM.nextInt(5)); // Assuming destination node ids range from 0 to 99
        }

        sourceNodeMap.put(sourceNodeId, destinationNodeIds);
      }

      table.put(eventType, sourceNodeMap);
    }

    return table;
  }

  // public static void main(String[] args) {
  //   // Generate a random table with 3 event types, 5 source nodes per event, and up to 10
  //   // destination nodes per source
  //   HashMap<String, HashMap<Integer, TreeSet<Integer>>> randomTable1 = generateRandomTable(3, 5,
  // 5);
  //   HashMap<String, HashMap<Integer, TreeSet<Integer>>> randomTable2 = generateRandomTable(3, 5,
  // 5);
  //   ForwardingTable tbl1 = new ForwardingTable();
  //   ForwardingTable tbl2 = new ForwardingTable();
  //   ForwardingTable tbl3 = new ForwardingTable();
  //   ForwardingTable tbl4 = new ForwardingTable();
  //   ForwardingTable tbl5 = new ForwardingTable();
  //   ForwardingTable tbl6 = new ForwardingTable();
  //   tbl1.table = randomTable1;
  //   tbl2.table = randomTable2;
  //   tbl3.table = randomTable2;
  //   tbl4.table = null;
  //   tbl6.table = null;
  //   tbl5.table = tbl1.table;
  //
  //   System.out.println(tbl1.table);
  //   System.out.println(tbl2.table);
  //   System.out.println(tbl3.table);
  //   System.out.println("tbl1 == tbl2 : " + tbl1.table.equals(tbl2.table));
  //   System.out.println("tbl2 == tbl3 : " + tbl2.table.equals(tbl3.table));
  //   System.out.println("tbl2 == tbl4 : " + tbl2.table.equals(tbl4.table));
  //   System.out.println(tbl4.table == tbl4.table);
  //   try {
  //     System.out.println("tbl4 == tbl4 : " + tbl4.table.equals(tbl4.table));
  //   } catch (NullPointerException e) {
  //     System.out.println("tbl4 == tbl4 : " + e.getMessage());
  //   }
  //   try {
  //     System.out.println("tbl4 == tbl5 : " + tbl4.table.equals(tbl5.table));
  //   } catch (NullPointerException e) {
  //     e.printStackTrace();
  //   }
  //   System.out.println("tbl5 == tbl1 : " + tbl5.table.equals(tbl1.table));
  //   System.out.println("tbl4 == tbl6 : " + tbl4.table.equals(tbl6.table));
  // }

  public static void main(String[] args) {

    ArrayList<String> oldDatabase = new ArrayList<>();
    AtomicBoolean flag = new AtomicBoolean(true);
    System.out.println("flag from main: " + flag);
    oldDatabase.add("Harrison Ford");
    oldDatabase.add("Carrie Fisher");
    oldDatabase.add("Mark Hamill");
    System.out.println("oldDatabase: " + oldDatabase.toString());
    ArrayList<String> newDatabase = new ArrayList<>();

    LocalDateTime twoSecondsLater = LocalDateTime.now().plusSeconds(2);
    Date twoSecondsLaterAsDate =
        Date.from(twoSecondsLater.atZone(ZoneId.systemDefault()).toInstant());
    System.out.println("Current time: " + LocalDateTime.now().toString());
    System.out.println("Fire time: " + twoSecondsLaterAsDate.toString());

    DatabaseMigrationTask task = new DatabaseMigrationTask(oldDatabase, newDatabase, flag);

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.schedule(task, 2, TimeUnit.SECONDS);
    scheduler.shutdown();

    System.out.println("Scheduled a task");
    oldDatabase.add("an unexpected item");

    // while (LocalDateTime.now().isBefore(twoSecondsLater)) {
    //   assert (newDatabase).isEmpty();
    //   try {
    //     Thread.sleep(500);
    //   } catch (InterruptedException e) {
    //     e.printStackTrace();
    //   }
    // }
    // do {} while (!oldDatabase.isEmpty());
    System.out.println("oldDatabase: " + oldDatabase.toString());
    System.out.println("newDatabase: " + newDatabase.toString());
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("flag from main: " + flag);
  }
}
