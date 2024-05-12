package com.huberlin.config;

public class NodeAddress {
  public String hostname;
  public int port;

  public String toString() {
    return hostname + ":" + port;
  }

  public String getEndpoint() {
    return hostname + ":" + port;
  }
}
