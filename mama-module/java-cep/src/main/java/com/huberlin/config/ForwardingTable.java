package com.huberlin.javacep.config;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardingTable implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(ForwardingTable.class);
  private final HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();

  /** Lookup destinations */
  public SortedSet<Integer> lookup(String event_type, Integer source) {
    SortedSet<Integer> result = this.table.getOrDefault(event_type, new HashMap<>()).get(source);
    if (result == null) {
      log.warn(
          "No entry in forwarding table for event type "
              + event_type
              + " and source id "
              + source
              + ". Returning empty destination set.");
      result = new TreeSet<>();
    }
    return new TreeSet<>(result);
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
  //   private final HashMap<String, HashMap<Integer, TreeSet<Integer>>> table = new HashMap<>();
}
