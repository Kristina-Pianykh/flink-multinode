#!/usr/bin/env bash

setup() {
  bats_load_library bats-assert
  INFLATED_RATES=1

  N_NODES=5
  FALLBACK_NODE=1
  NON_FALLBACK_NODES=(0 2)
  APPLY_STRATEGY=1
  OP=SEQ

  # TOPOLOGY=${OP}_ABC_${N_NODES}
  TOPOLOGY=SEQ_ABC_complex_5
  TRACE_DIR=/Users/krispian/Uni/bachelorarbeit/topologies/${TOPOLOGY}/plans/trace_inflated_${INFLATED_RATES}
  LOG_DIR=/Users/krispian/Uni/bachelorarbeit/topologies/${TOPOLOGY}/plans/output_strategy_${APPLY_STRATEGY}
}

@test "check if the log path exists" {
  if [ ! -d "$LOG_DIR" ]; then
    echo "Log directory $LOG_DIR does not exist"
    exit 1
  fi
  echo $LOG_DIR
  # run ls "$LOG_DIR"
  # run echo $LOG_DIR
  # [ "$status" -eq 0 ]
}

@test "check if the trace path exists" {
  if [ ! -d "$TRACE_DIR" ]; then
    echo "Trace directory $TRACE_DIR does not exist"
    exit 1
  fi
  echo $TRACE_DIR
  # run ls "$TRACE_DIR"
  # run echo $TRACE_DIR
  # [ "$status" -eq 0 ]
}

@test "assert non-fallback nodes are set" {
  echo "${NON_FALLBACK_NODES[@]}"
  run echo "${NON_FALLBACK_NODES[@]}"
  assert_output
}

@test "assert num of nodes is set" {
  echo "${NON_FALLBACK_NODES[@]}"
  run echo "${NON_FALLBACK_NODES[@]}"
  assert_output
}

@test "assert fallback node is set" {
  echo "${FALLBACK_NODE}"
  run echo "${FALLBACK_NODE}"
  assert_output
}

@test 'assert every complex event has ID()' {
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    lines_without_id=$(rg -S -e ".*complex \| [^|]*\:[^|]* \| .*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')
    echo "Number of complex events without ID: $lines_without_id"
    assert_equal "$lines_without_id" "0"
  done;
}

@test 'assert no duplicate simple events()' {
  # number of simple events sent to the monitor
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    total=$(rg -S -e "Received event: simple.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | wc -l)
    echo "  Total: $total"

    # distinct number
    distinct=$(rg -S -e "Received event: simple.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | awk '!seen[$0]++'| wc -l)
    echo "  Distinct: $distinct"

    assert_equal "$total" "$distinct"
  done;
}

@test 'assert no duplicate complex events()' {
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    # number of simple events sent to the monitor
    total=$(rg -S -e "Received event: complex.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}'| wc -l)
    echo "  Total: $total"

    # distinct number
    distinct=$(rg -S -e "Received event: complex.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | awk '!seen[$0]++'| wc -l)
    echo "  Distinct: $distinct"

    assert_equal "$total" "$distinct"
  done;
}


@test 'events processed total()' {
  simple_total=0
  complex_total=0
  total=0

  for ((i=0; i<N_NODES; i++)); do
    simple_events_per_node=0
    complex_events_per_node=0

    simple=$(rg -S -e ".*com\.huberlin\.javacep\.communication\.TCPEventSender.*SendToMonitor: simple.*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')
    complex=$(rg -S -e ".*com\.huberlin\.javacep\.communication\.TCPEventSender.*SendToMonitor: complex.*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')

    echo "Node $i"
    echo "  Processed:"
    echo "    simple events: $simple"
    echo "    complex events: $complex"
    echo "    total: $((simple+complex))"

    simple_total=$((simple_total+simple))
    complex_total=$((complex_total+complex))
  done;

  echo ""
  echo "  Processed total:"
  echo "    Simple events: $simple_total"
  echo "    Complex events : $complex_total"
  echo "    Absolute total: $((simple_total+complex_total))"
}


@test 'complex multisink events created ()' {
  for i in "${NON_FALLBACK_NODES[@]}"; do
    echo "Node $i"
    count=$(rg -S -e 'Complex event created:' ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')
    echo "Complex events generated by non-fallback node $i: $count"
    # assert [ "$count" -gt 0 ]
  done;

  count=$(rg -S -e 'Complex event created:' ${LOG_DIR}/$FALLBACK_NODE.log | wc -l | tr -d '[:space:]')
  echo "Complex events generated by fallback node $FALLBACK_NODE: $count"
  # assert [ "$count" -gt 0 ]
}

# @test 'matches for multi-sink q on non-fallback nodes after switch()' {
#   # NON_FALLBACK_NODES=(0 4)
#   for i in "${NON_FALLBACK_NODES[@]}"; do
#     echo "Node $i"
#     line=$(rg -n -S -e "FLUSHING" ${LOG_DIR}/$i.log | awk -F  ':' '{print $1}')
#     if [ -z "$line" ]; then
#       echo "No flushing on $i"
#       continue
#     fi
#
#     next_line=$((line+1))
#     count=$(tail -n +$next_line ${LOG_DIR}/$i.log | rg -S -e 'Complex event created:.*SEQ\(., ., .\).*' | wc -l | tr -d '[:space:]')
#     echo "Complex events created on non-fallback node $i after disabling the multi-sink query: $count"
#     # run bash -c "tail -n +$next_line ${LOG_DIR}/$i.log | rg -S -e '.*complex.*SEQ\(., ., .\).*' | wc -l"
#     # assert_equal "$count" "0"
#     # refute_output
#   done;
# }

@test 'flushed events receieved by fallback node()' {
  # NON_FALLBACK_NODES=(0 4)
  OLDIFS=$IFS

  for i in "${NON_FALLBACK_NODES[@]}"; do
    echo "Node $i"
    IFS=$OLDIFS

    events=$(rg -n -S -e "FLUSHING" ${LOG_DIR}/$i.log | awk -F ': ' '{print $3}' | tr -d '[]')
    IFS=','
    total_cnt=0
    fail_cnt=0

    for event in $events; do

      total_cnt=$((total_cnt+1))

      id=$(echo $event | awk '{print $3}')
      rg -q $id ${LOG_DIR}/${FALLBACK_NODE}.log
      # echo $id | xargs -I {} rg "{}" ${LOG_DIR}/${FALLBACK_NODE}.log > /dev/null

      # assert_success
      if [ $? -ne 0 ]; then
        echo "Event $id send by $i was NOT received by the fallback node $FALLBACK_NODE";
        fail_cnt=$((fail_cnt+1))
      fi

    done
    echo "Total flushed events sent by $i: $total_cnt"
    echo "Flushed events failed to deliver to $FALLBACK_NODE: $fail_cnt"
    assert_equal "$fail_cnt" "0"

  done;


  IFS=$OLDIFS
}


@test 'failed to arrive local events()' {
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    total=$(cat ${TRACE_DIR}/trace_$i.csv | wc -l | tr -d '[:space:]')
    fail_cnt=$(python /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/tests/check_ids.py --trace ${TRACE_DIR}/trace_$i.csv --log ${LOG_DIR}/$i.log | tr -d '[:space:]')
    echo "  Local events failed to arrive to $i: $fail_cnt/$total"
  done;
}


@test 'events sent total()' {
  simple_total=0
  complex_total=0
  total=0

  for ((i=0; i<N_NODES; i++)); do
    simple_events_per_node=0
    complex_events_per_node=0

    simple=$(rg -S -e ".*com\.huberlin\.javacep\.communication\.TCPEventSender.*sendTo\(\)\: forwarding message.*simple.*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')
    complex=$(rg -S -e ".*com\.huberlin\.javacep\.communication\.TCPEventSender.*sendTo\(\)\: forwarding message.*complex.*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')

    echo "Node $i"
    echo "  Sent:"
    echo "    simple events: $simple"
    echo "    complex events: $complex"
    echo "    total: $((simple+complex))"

    simple_total=$((simple_total+simple))
    complex_total=$((complex_total+complex))
  done;

  echo ""
  echo "  Sent total:"
  echo "    Simple events: $simple_total"
  echo "    Complex events: $complex_total"
  echo "    Absolute total: $((simple_total+complex_total))"
}
