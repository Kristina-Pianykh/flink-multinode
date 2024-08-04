package com.huberlin.test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatabaseMigrationTask implements Runnable {
  public ArrayList<String> oldDatabase;
  public ArrayList<String> newDatabase;
  public AtomicBoolean flag;

  public DatabaseMigrationTask(
      ArrayList<String> oldDatabase, ArrayList<String> newDatabase, AtomicBoolean flag) {
    this.oldDatabase = oldDatabase;
    this.newDatabase = newDatabase;
    this.flag = flag;
    System.out.println("DatabaseMigrationTask created");
  }

  @Override
  public void run() {
    newDatabase.addAll(oldDatabase);
    System.out.println("Database migrated");
    oldDatabase.clear();
    flag.set(false);
    System.out.println("oldDatabase : " + oldDatabase.toString());
    System.out.println("newDatabase : " + newDatabase.toString());
    System.out.println("flag from scheduled task: " + flag.toString());
  }
}
