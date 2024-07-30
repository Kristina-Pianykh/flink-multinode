package com.huberlin.test;

import java.util.*;

public class DatabaseMigrationTask extends TimerTask {
  public ArrayList<String> oldDatabase;
  public ArrayList<String> newDatabase;
  public Timer timer;

  public DatabaseMigrationTask(
      ArrayList<String> oldDatabase, ArrayList<String> newDatabase, Timer timer) {
    this.oldDatabase = oldDatabase;
    this.newDatabase = newDatabase;
    System.out.println("DatabaseMigrationTask created");
    this.timer = timer;
  }

  @Override
  public void run() {
    newDatabase.addAll(oldDatabase);
    oldDatabase.clear();
    timer.cancel();
    timer.purge();
  }
}
