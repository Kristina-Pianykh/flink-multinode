package com.huberlin.javacep.config;

import java.io.Serializable;

public class NodeAddress implements Serializable {
  public String hostname;
  public int port;

  public String toString() {
    return hostname + ":" + port;
  }

  public String getEndpoint() {
    return hostname + ":" + port;
  }
}
