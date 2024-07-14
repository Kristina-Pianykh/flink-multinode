// package monitor;
package com.huberlin.monitor;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.cli.*;
import org.json.JSONObject;

public class JsonParser {

  public static Integer getMonitorPort(String path, String nodeId) throws IOException {
    String jsonString =
        new String(Files.readAllBytes(Paths.get(path))); // global config (address book)
    JSONObject obj = new JSONObject(jsonString);
    Integer port = Integer.parseInt(obj.getString(nodeId).split(":")[1]);
    return port + 20;
  }
}
