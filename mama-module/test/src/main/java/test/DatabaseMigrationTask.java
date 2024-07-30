package com.huberlin.test;

import java.util.*;

public class DatabaseMigrationTask implements Runnable {
  public ArrayList<String> oldDatabase;
  public ArrayList<String> newDatabase;

  public DatabaseMigrationTask(ArrayList<String> oldDatabase, ArrayList<String> newDatabase) {
    this.oldDatabase = oldDatabase;
    this.newDatabase = newDatabase;
    System.out.println("DatabaseMigrationTask created");
  }

  @Override
  public void run() {
    newDatabase.addAll(oldDatabase);
    System.out.println("Database migrated");
    oldDatabase.clear();
    System.out.println("oldDatabase : " + oldDatabase.toString());
    System.out.println("newDatabase : " + newDatabase.toString());
  }
}
