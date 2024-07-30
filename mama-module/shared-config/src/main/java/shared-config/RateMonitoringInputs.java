package com.huberlin.sharedconfig;

import java.io.*;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;

public class RateMonitoringInputs implements Serializable {
  public String multiSinkQuery;
  public List<Integer> multiSinkNodes;
  public int fallbackNode;
  public int numMultiSinkNodes;
  public String partitioningInput;
  public List<String> queryInputs;
  public List<String> nonPartitioningInputs;
  public int steinerTreeSize;
  public HashMap<String, Integer> numNodesPerQueryInput;

  public static RateMonitoringInputs parseRateMonitoringInputs(String filePath) {
    RateMonitoringInputs rateMonitoringInputs = new RateMonitoringInputs();
    try {
      String jsonString = new String(Files.readAllBytes(Paths.get(filePath)));
      JSONObject jsonObject = new JSONObject(jsonString);
      rateMonitoringInputs.multiSinkQuery = jsonObject.getString("multiSinkQuery");
      rateMonitoringInputs.multiSinkNodes =
          jsonArrayToInt(jsonObject.getJSONArray("multiSinkNodes"));
      rateMonitoringInputs.fallbackNode = jsonObject.getInt("fallbackNode");
      rateMonitoringInputs.numMultiSinkNodes = jsonObject.getInt("numMultiSinkNodes");
      rateMonitoringInputs.partitioningInput = jsonObject.getString("partitioningInput");
      rateMonitoringInputs.queryInputs = jsonArrayToList(jsonObject.getJSONArray("queryInputs"));
      rateMonitoringInputs.nonPartitioningInputs =
          jsonArrayToList(jsonObject.getJSONArray("nonPartitioningInputs"));
      rateMonitoringInputs.steinerTreeSize = jsonObject.getInt("steinerTreeSize");
      rateMonitoringInputs.numNodesPerQueryInput = new HashMap<>();
      JSONObject numNodesPerQueryInput = jsonObject.getJSONObject("numNodesPerQueryInput");
      for (String key : numNodesPerQueryInput.keySet()) {
        rateMonitoringInputs.numNodesPerQueryInput.put(key, numNodesPerQueryInput.getInt(key));
      }
    } catch (IOException e) {
      System.err.println("Error reading JSON file: " + e.getMessage());
    }
    return rateMonitoringInputs;
  }

  @Override
  public String toString() {
    return "RateMonitoringInputs:\n"
        + "   multiSinkQuery = "
        + multiSinkQuery
        + "\n   multiSinkNodes = "
        + multiSinkNodes
        + "\n   fallbackNode = "
        + fallbackNode
        + "\n   numMultiSinkNodes = "
        + numMultiSinkNodes
        + "\n   partitioningInput = "
        + partitioningInput
        + "\n   queryInputs = "
        + queryInputs
        + "\n   nonPartitioningInputs = "
        + nonPartitioningInputs
        + "\n   steinerTreeSize = "
        + steinerTreeSize
        + "\n   numNodesPerQueryInput = "
        + numNodesPerQueryInput;
  }

  public boolean isMultisinkNode(int nodeId) {
    if (multiSinkNodes.contains(nodeId)) return true;
    else return false;
  }

  public boolean isFallbackNode(int nodeId) {
    if (isMultisinkNode(nodeId) && nodeId == this.fallbackNode) return true;
    else return false;
  }

  public boolean isNonFallbackNode(int nodeId) {
    if (isMultisinkNode(nodeId) && nodeId != this.fallbackNode) return true;
    else return false;
  }

  private static List<String> jsonArrayToList(JSONArray jsonArray) {
    return jsonArray.toList().stream().map(Object::toString).collect(Collectors.toList());
  }

  private static List<Double> jsonArrayToDoubleList(JSONArray jsonArray) {
    return jsonArray.toList().stream()
        .map(Object::toString)
        .map(Double::parseDouble)
        .collect(Collectors.toList());
  }

  private static List<Integer> jsonArrayToInt(JSONArray jsonArray) {
    return jsonArray.toList().stream()
        .map(Object::toString)
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }
}
