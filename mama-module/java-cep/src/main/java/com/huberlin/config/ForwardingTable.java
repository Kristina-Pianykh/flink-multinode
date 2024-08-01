package com.huberlin.javacep.config;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardingTable implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ForwardingTable.class);
  // private final HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();
  public HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();

  /** Lookup destinations */
  public SortedSet<Integer> lookup(String event_type, Integer source) {
    SortedSet<Integer> result = this.table.getOrDefault(event_type, new HashMap<>()).get(source);
    if (result == null) {
      LOG.warn(
          "No entry in forwarding table for event type "
              + event_type
              + " and source id "
              + source
              + ". Returning empty destination set.");
      result = new TreeSet<>();
    }
    return new TreeSet<>(result);
  }

  // lookup destinctions for the updated forwarding table (containes null as src node)
  // the input arg is supposed to be the partitioning event type
  // TODO: refactor to use one func for regular fwd table and the updated one
  public SortedSet<Integer> lookupUpdated(String eventType) {
    SortedSet<Integer> dest = new TreeSet<>();

    HashMap<Integer, TreeSet<Integer>> srcDestMap =
        this.table.getOrDefault(eventType, new HashMap<>());
    if (srcDestMap.isEmpty()) return dest;

    System.out.println(srcDestMap);
    for (SortedSet<Integer> set : srcDestMap.values()) {
      dest.addAll(set);
    }
    // dest.addAll(this.table.getOrDefault(eventType, new HashMap<>()).get(null));
    System.out.println(dest);

    if (dest == null) {
      LOG.warn(
          "No entry in forwarding table for event type "
              + eventType
              + ". Returning empty destination set.");
    }
    return dest;
  }

  /**
   * Extend the list of destinations for a given tuple of source and event type
   *
   * @return true if at least one destination was added
   */
  boolean addAll(String event_type, Integer source_node_id, Collection<Integer> destinations) {
    this.table.putIfAbsent(event_type, new HashMap<>());
    this.table.get(event_type).putIfAbsent(source_node_id, new TreeSet<>());
    return this.table.get(event_type).get(source_node_id).addAll(destinations);
  }

  void addUpdatedAll(ArrayList<HashMap<String, Integer>> rules) {
    for (HashMap<String, Integer> rule : rules) {
      for (Map.Entry<String, Integer> entry : rule.entrySet()) {
        String event_type = entry.getKey();
        Integer dstNode = entry.getValue();
        this.table.putIfAbsent(event_type, new HashMap<>());
        this.table
            .get(event_type)
            .putIfAbsent(
                null,
                new TreeSet<>()); // null because we don't care about where the event comes from
        this.table.get(event_type).get(null).add(dstNode);
      }
    }
  }

  /**
   * Get all node ids mentioned in table
   *
   * @return
   */
  public SortedSet<Integer> get_all_node_ids() {
    SortedSet<Integer> result = new TreeSet<>();
    result.addAll(this.get_all_sources());
    result.addAll(this.getAllDestinations());
    return result;
  }

  SortedSet<Integer> get_all_sources() {
    return this.table.values().stream()
        .flatMap((src_id_to_dest_map) -> src_id_to_dest_map.keySet().stream())
        .collect(Collectors.toCollection(TreeSet::new));
  }

  SortedSet<Integer> getAllDestinations() {
    return this.table.values().stream()
        .flatMap(
            (src_id_to_dest_map) ->
                src_id_to_dest_map.values().stream()
                    .flatMap((destinations) -> destinations.stream()))
        .collect(Collectors.toCollection(TreeSet::new));
  }

  @Override
  public String toString() {
    if (this.table.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (String eventType : this.table.keySet()) {
      HashMap<Integer, TreeSet<Integer>> map = this.table.get(eventType);
      for (Integer source_id : map.keySet()) {
        TreeSet<Integer> destinations = map.get(source_id);
        for (Integer destination_id : destinations) {
          sb.append(
              "Event type: "
                  + eventType
                  + " source ID: "
                  + source_id
                  + " dest ID: "
                  + destination_id
                  + "\n");
        }
      }
    }
    return sb.toString();
  }

  public void print() {
    if (this.table.isEmpty()) {
      System.out.println("Forwarding table is empty.");
    }
    for (String eventType : this.table.keySet()) {
      HashMap<Integer, TreeSet<Integer>> map = this.table.get(eventType);
      for (Integer source_id : map.keySet()) {
        TreeSet<Integer> destinations = map.get(source_id);
        for (Integer destination_id : destinations) {
          System.out.println(
              "Event type: "
                  + eventType
                  + " source ID: "
                  + source_id
                  + " dest ID: "
                  + destination_id);
        }
      }
    }
  }

  public HashSet<Integer> toHashSet(SortedSet<Integer> dsts) {
    HashSet<Integer> set = new HashSet<>();
    for (Integer dst : dsts) {
      set.add(dst);
    }
    return set;
  }

  @Override
  public boolean equals(Object other) {
    // HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>(); event type -->
    // source node id --> destination node ids
    if (other == null) {
      LOG.warn("RHS ForwardingTable is null");
      return false;
    }

    try {
      if (!(other instanceof ForwardingTable)) {
        LOG.debug("ForwardingTable: other is of type {}", other.getClass());
        return false;
      }

      if (other == this) {
        LOG.debug("ForwardingTable: other is the same object as this");
        return true;
      }

      ForwardingTable otherTable = (ForwardingTable) other;
      if (this.table.size() != otherTable.table.size()) {
        LOG.debug("ForwardingTable: table sizes are different");
        return false;
      }

      for (String eventType : this.table.keySet()) {
        LOG.debug("Checking event type {} in LHS ForwardingTable", eventType);
        if (!otherTable.table.keySet().contains(eventType)) {
          LOG.debug("Event type {} not in RHS table", eventType);
          return false;
        } else {
          HashMap<Integer, TreeSet<Integer>> thisSrcDestMap = this.table.get(eventType);
          if (thisSrcDestMap.size() != otherTable.table.get(eventType).size()) {
            LOG.debug("Number of forwarding rules for event type {} is different", eventType);
            return false;
          }
          HashMap<Integer, TreeSet<Integer>> thatSrcDestMap = otherTable.table.get(eventType);

          for (Integer srcNode : thisSrcDestMap.keySet()) {
            LOG.debug("Checking source node {} in LHS ForwardingTable", srcNode);
            if (!thatSrcDestMap.keySet().contains(srcNode)) {
              LOG.debug("Source node {} not in RHS table", srcNode);
              return false;
            }
            if (thisSrcDestMap.get(srcNode).size() != thatSrcDestMap.get(srcNode).size()) {
              LOG.debug(
                  "Number of destinations for source node {} and event type {} is different",
                  srcNode,
                  eventType);
              return false;
            } else {
              HashSet thisDsts = toHashSet(thisSrcDestMap.get(srcNode));
              HashSet thatDsts = toHashSet(thatSrcDestMap.get(srcNode));
              LOG.debug(
                  "Comparing destinations for source node {} and event type {}",
                  srcNode,
                  eventType);
              LOG.debug("LHS destinations: {}", thisDsts);
              LOG.debug("RHS destinations: {}", thatDsts);
              if (!thisDsts.equals(thatDsts)) {
                LOG.debug(
                    "Destinations for source node {} and event type {} are different",
                    srcNode,
                    eventType);
                return false;
              }
            }
          }
        }
      }
    } catch (NullPointerException e) {
      LOG.error("Error while comparing forwarding tables: " + e.getMessage());
      LOG.error("LHS forwarding table is null");
      return false;
    }

    return true;
  }
}
