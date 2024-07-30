package com.huberlin.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ScheduledExecutorService;

public class Test {

  public static void main(String[] args) {

    ArrayList<String> oldDatabase = new ArrayList<>();
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

    DatabaseMigrationTask task = new DatabaseMigrationTask(oldDatabase, newDatabase);

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.schedule(task, 2, TimeUnit.SECONDS);

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
    if (scheduler.isTerminated()) {
      System.out.println("Scheduler terminated");
      scheduler.shutdown();
    }
  }
}
