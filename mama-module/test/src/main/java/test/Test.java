package com.huberlin.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

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

    Timer timer = new Timer();
    DatabaseMigrationTask task = new DatabaseMigrationTask(oldDatabase, newDatabase, timer);
    // task.oldDatabase = oldDatabase;
    // task.newDatabase = newDatabase;
    // task.timer = timer;

    timer.schedule(task, twoSecondsLaterAsDate);
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
    // while (true) {
    //   System.out.println("oldDatabase: " + oldDatabase.toString());
    //   if (oldDatabase.isEmpty()) {
    //     timer.cancel();
    //     break;
    //   }
    // }
    // do {} while (!oldDatabase.isEmpty());
    System.out.println("oldDatabase: " + oldDatabase.toString());
    System.out.println("newDatabase: " + newDatabase.toString());
  }
}
