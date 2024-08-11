package com.huberlin.javacep.config;

import com.huberlin.javacep.communication.addresses.TCPAddressString;
import com.huberlin.sharedconfig.RateMonitoringInputs;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeConfig implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(NodeConfig.class);

  public Forwarding forwarding;
  public List<Processing> processing;
  public int nodeId;
  public RateMonitoringInputs rateMonitoringInputs;
  public NodeAddress hostAddress;

  public static class Forwarding implements Serializable {
    public final HashMap<Integer, NodeAddress> addressBook = new HashMap<>();
    public HashMap<Integer, TCPAddressString> addressBookTCP;
    public AtomicReference<ForwardingTable> table = new AtomicReference(new ForwardingTable());
    public ForwardingTable updatedTable = new ForwardingTable();
    public ArrayList<Integer> recipient;
  }

  public static class Processing implements Serializable {
    public String queryName;
    public List<String> subqueries;
    public long queryLength;
    public List<String> output_selection;
    public List<List<String>> inputs;
    public List<Double> selectivities;
    public List<List<List<String>>> sequenceConstraints;
    public List<List<String>> idConstraints;
    public long timeWindowSize;
    public long predicate_checks;
    public int is_negated;
    public List<String> context;
    public int kleene_type;
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

  private static void print(NodeConfig.Processing p) {
    System.out.println("Processing:");
    System.out.println("  Query name: " + p.queryName);
    //    System.out.println("  Output selection: " + p.output_selection);
    System.out.println("  Inputs: " + p.inputs);
    System.out.println("  Sequence constraints: " + p.sequenceConstraints);
    System.out.println("  ID constraints: " + p.idConstraints);
    System.out.println("  Time window size: " + p.timeWindowSize);
    System.out.println("  Predicate checks: " + p.predicate_checks);
    System.out.println("  Context: " + p.context);
    System.out.println("  Negated: " + p.is_negated);
    System.out.println("  Kleene Type: " + p.kleene_type);
  }

  // public static void main(String[] args) throws IOException {
  //   String filePath_local =
  //
  // "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/example_inputs/multiquery/config_0.json";
  //   String filePath_global =
  //
  // "/Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json";
  //   String rateMonitoringInputsPath =
  //
  // "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs/generate_flink_inputs/plans/inequality_inputs.json";
  //
  //   NodeConfig config = new NodeConfig();
  //   config.parseJsonFile(filePath_local, filePath_global, rateMonitoringInputsPath);
  // }

  public void parseJsonFile(
      String local_config,
      String global_config,
      String rateMonitoringInputsPath,
      String updatedForwardingRulesPath)
      throws IOException {
    try {
      String jsonString =
          new String(Files.readAllBytes(Paths.get(local_config))); //  local config (address book)
      JSONObject local = new JSONObject(jsonString);
      String jsonString2 =
          new String(Files.readAllBytes(Paths.get(global_config))); // global config (address book)
      JSONObject global = new JSONObject(jsonString2);
      String jsonString3 = new String(Files.readAllBytes(Paths.get(updatedForwardingRulesPath)));
      JSONObject updatedRules = new JSONObject(jsonString3);

      //            NodeConfig nodeConfig = new NodeConfig();
      this.nodeId = local.getJSONObject("forwarding").getInt("node_id");
      System.out.println("node_id: " + this.nodeId);

      parseForwarding(local, global, updatedRules);
      parseProcessing(local);
      this.rateMonitoringInputs =
          RateMonitoringInputs.parseRateMonitoringInputs(rateMonitoringInputsPath);
      assert this.rateMonitoringInputs != null : "Failed to parse rateMonitoringInputs";
      System.out.println("RateMonitoringInputs: " + this.rateMonitoringInputs);

    } catch (IOException e) {
      System.err.println("Error reading JSON file: " + e.getMessage());
      throw e;
    }
  }

  private HashMap<Integer, TCPAddressString> parseAddressBook(
      Map<Integer, NodeAddress> address_book, ForwardingTable fwd_table, int nodeid) {
    HashMap<Integer, TCPAddressString> addressBookTCP = new HashMap<>();
    // TODO: move logic to initialization of fwd table and address book
    // or at least in open() using richSinkFunction
    // check that address book contains entries for all node ids
    for (Integer node_id : fwd_table.get_all_node_ids())
      if (!address_book.containsKey(node_id))
        throw new IllegalArgumentException(
            "The address book does not have an entry for the node ID " + node_id);
    // convert node ids to tcp address strings in address book
    for (Integer node_id : address_book.keySet())
      addressBookTCP.put(node_id, new TCPAddressString(address_book.get(node_id).getEndpoint()));

    return addressBookTCP;
  }

  private void parseForwarding(JSONObject local, JSONObject address_book, JSONObject updatedRules) {
    JSONObject forwardingObject = local.getJSONObject("forwarding");
    this.forwarding = new NodeConfig.Forwarding();
    try {
      assert this.forwarding != null;
    } catch (AssertionError e) {
      LOG.error("Forwarding object failed to initialize: {}", e.getMessage());
      throw e;
    }

    // read forwarding table
    JSONArray ftJson = forwardingObject.getJSONArray("forwarding_table");
    for (int i = 0; i < ftJson.length(); i++) {
      JSONArray ftEntry =
          ftJson.getJSONArray(
              i); // form of an entry: [event_type, list_of_sources, list_of_destinations]
      String eventType = ftEntry.getString(0);
      List<Integer> sourceNodes = jsonArrayToInt(ftEntry.getJSONArray(1));
      List<Integer> destNodes = jsonArrayToInt(ftEntry.getJSONArray(2));

      //            List<Integer> sourceNodes = ftEntry.getJSONArray(1).toList().stream().map(nodeId
      // -> (Integer) nodeId).collect(Collectors.toList());
      //            List<Integer> destNodes = ftEntry.getJSONArray(2).toList().stream().map(nodeId
      // -> (Integer) nodeId).collect(Collectors.toList());
      System.out.println(
          "eventType: "
              + eventType
              + "; sourceNodes: "
              + sourceNodes
              + "; destNodes: "
              + destNodes);

      for (Integer sourceNode : sourceNodes)
        // this.forwarding.table.addAll(eventType, sourceNode, destNodes);
        this.forwarding.table.get().addAll(eventType, sourceNode, destNodes);
    }

    if (!updatedRules.keySet().contains(Integer.toString(this.nodeId))) {
      System.out.println("==================================================");
      System.out.println("No updated rules for node " + this.nodeId);
      this.forwarding.table.get().print();
      System.out.println("==================================================");
      this.forwarding.updatedTable = this.forwarding.table.get();
    } else {
      JSONArray rulesTmp = updatedRules.getJSONArray(Integer.toString(this.nodeId));
      ArrayList<HashMap<String, Integer>> rules =
          rulesTmp.toList().stream()
              .map(rule -> (HashMap<String, Integer>) rule)
              .collect(Collectors.toCollection(ArrayList::new));
      this.forwarding.updatedTable.addUpdatedAll(rules);
      System.out.println("==================updated table==================");
      this.forwarding.updatedTable.print();
      System.out.println("==================================================");
    }

    // this.forwarding.table.addAll(eventType, sourceNode, destNodes);

    SortedSet<Integer> allDestNodes = this.forwarding.table.get().getAllDestinations();
    System.out.println("All destination nodes: " + allDestNodes);

    this.forwarding.recipient = new ArrayList<>(this.forwarding.table.get().getAllDestinations());
    System.out.println("forwarding.recipient: " + this.forwarding.recipient);

    // address book
    // TODO: refactor to set attributes for host and port separately
    for (String nodeIdAsStr : address_book.keySet()) {
      int nodeId = Integer.parseInt((nodeIdAsStr).trim());
      NodeAddress nodeAddress = new NodeAddress();
      String endpoint = address_book.getString(nodeIdAsStr);
      if (!endpoint.contains(":")) {
        LOG.error("Endpoint for Node ID " + nodeIdAsStr + "in address book does not contain ':'");
        System.exit(-1);
      }
      nodeAddress.hostname = endpoint.split(":")[0];
      nodeAddress.port = Integer.parseInt(endpoint.split(":")[1]);
      this.forwarding.addressBook.put(nodeId, nodeAddress);

      if (this.nodeId == nodeId) {
        this.hostAddress = nodeAddress;
      }
    }

    System.out.println("Forwarding:");
    System.out.println("  Table: ");
    this.forwarding.table.get().print();
    System.out.println("  Address book: " + this.forwarding.addressBook);
    this.forwarding.addressBookTCP =
        parseAddressBook(
            this.forwarding.addressBook, this.forwarding.table.getPlain(), this.nodeId);
    System.out.println("  Address book TCP: " + this.forwarding.addressBookTCP);
  }

  private void parseProcessing(JSONObject local) {
    JSONArray queriesJsonObj = local.getJSONArray("processing");
    this.processing = new ArrayList<>();

    for (int i = 0; i < queriesJsonObj.length(); i++) {
      NodeConfig.Processing query = new NodeConfig.Processing();
      JSONObject queryObject = queriesJsonObj.getJSONObject(i);

      query.queryName = queryObject.getString("query_name");

      query.subqueries = jsonArrayToList(queryObject.getJSONArray("subqueries"));
      query.queryLength = queryObject.getLong("query_length");
      query.output_selection = jsonArrayToList(queryObject.getJSONArray("output_selection"));
      query.context = jsonArrayToList(queryObject.getJSONArray("context"));
      query.is_negated = queryObject.getInt("is_negated");
      query.kleene_type = queryObject.getInt("kleene_type");

      JSONArray inputs = queryObject.getJSONArray("inputs");
      query.inputs = new ArrayList<>();
      for (int j = 0; j < inputs.length(); j++) {
        List<String> input = jsonArrayToList(inputs.getJSONArray(j));
        query.inputs.add(input);
      }

      query.selectivities = jsonArrayToDoubleList(queryObject.getJSONArray("selectivities"));

      JSONArray sequenceConstraintsArray = queryObject.getJSONArray("sequence_constraints");
      query.sequenceConstraints = new ArrayList<>();

      for (int j = 0; j < sequenceConstraintsArray.length(); j++) {
        List<List<String>> ConstraintPerSubquery = new ArrayList<>();
        JSONArray ConstraintPerSubqueryObj = sequenceConstraintsArray.getJSONArray(j);
        for (int k = 0; k < ConstraintPerSubqueryObj.length(); k++) {
          ConstraintPerSubquery.add(jsonArrayToList(ConstraintPerSubqueryObj.getJSONArray(k)));
        }
        query.sequenceConstraints.add(ConstraintPerSubquery);
      }

      JSONArray constraints = queryObject.getJSONArray("id_constraints"); // TODO check for saneness
      query.idConstraints = new ArrayList<>();
      for (int j = 0; j < constraints.length(); j++) {
        query.idConstraints.add(jsonArrayToList(constraints.getJSONArray(j)));
      }

      query.timeWindowSize = queryObject.getLong("time_window_size");
      query.predicate_checks = queryObject.getInt("predicate_checks");

      /*if (Event.getPrimitiveTypes(q.query_name).size() != q.query_length)
      throw new IllegalArgumentException("Query given as " + q.query_name
              + " does not match query_length, given as " + q.query_length);*/

      print(query);
      this.processing.add(query);
    }
  }
}
