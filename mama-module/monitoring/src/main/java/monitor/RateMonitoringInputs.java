// package monitor;
package com.huberlin.monitor;

import java.io.Serializable;
import java.util.*;

public class RateMonitoringInputs implements Serializable {
  public String multiSinkQuery;
  public List<Integer> multiSinkNodes;
  public int numMultiSinkNodes;
  public String partitioningInput;
  public List<String> queryInputs;
  public List<String> nonPartitioningInputs;
  public int steinerTreeSize;
  public HashMap<String, Integer> numNodesPerQueryInput;

  @Override
  public String toString() {
    return "RateMonitoringInputs{"
        + "multiSinkQuery='"
        + multiSinkQuery
        + '\''
        + ", multiSinkNodes="
        + multiSinkNodes
        + ", numMultiSinkNodes="
        + numMultiSinkNodes
        + ", partitioningInput='"
        + partitioningInput
        + '\''
        + ", queryInputs="
        + queryInputs
        + ", nonPartitioningInputs="
        + nonPartitioningInputs
        + ", steinerTreeSize="
        + steinerTreeSize
        + ", numNodesPerQueryInput="
        + numNodesPerQueryInput
        + '}';
  }
}
