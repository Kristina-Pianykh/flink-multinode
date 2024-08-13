#!/usr/bin/env bash

setup() {
  bats_load_library bats-assert
  N_NODES=5
}

@test "check if the log path exists" {
  run ls "$LOG_DIR"
  echo $LOG_DIR
  [ "$status" -eq 0 ]
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

@test 'assert no duplicate simple events()' {
  # number of simple events sent to the monitor
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    total=$(rg -S -e "Received event: simple.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | wc -l)
    echo "Total: $total"

    # distinct number
    distinct=$(rg -S -e "Received event: simple.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | awk '!seen[$0]++'| wc -l)
    echo "Distinct: $distinct"

    assert_equal "$total" "$distinct"
  done;
}

@test 'assert every complex event has ID()' {
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    lines_without_id=$(rg -S -e ".*complex \| [^|]*\:[^|]* \| .*" ${LOG_DIR}/$i.log | wc -l | tr -d '[:space:]')
    echo "Number of complex events without ID: $lines_without_id"
    assert_equal "$lines_without_id" "0"
  done;
}

@test 'assert no duplicate complex events()' {
  for ((i=0; i<N_NODES; i++)); do
    echo "Node $i"
    # number of simple events sent to the monitor
    total=$(rg -S -e "Received event: complex.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}'| wc -l)
    echo "Total: $total"

    # distinct number
    distinct=$(rg -S -e "Received event: complex.*" ${LOG_DIR}/$i.log | awk -F  '|' '{print $2}' | awk '!seen[$0]++'| wc -l)
    echo "Distinct: $distinct"

    assert_equal "$total" "$distinct"
  done;
}

@test 'matches for multi-sink q on non-fallback nodes after switch()' {
  NON_FALLBACK_NODES=(0 4)
  for i in "${NON_FALLBACK_NODES[@]}"; do
    echo "Node $i"
    line=$(rg -n -S -e "FLUSHING" ${LOG_DIR}/$i.log | awk -F  ':' '{print $1}')
    next_line=$((line+1))
    count=$(tail -n +$next_line ${LOG_DIR}/$i.log | rg -S -e 'Complex event created:.*SEQ\(., ., .\).*' | wc -l | tr -d '[:space:]')
    echo "Complex events created on non-fallback node $i after disabling the multi-sink query: $count"
    # run bash -c "tail -n +$next_line ${LOG_DIR}/$i.log | rg -S -e '.*complex.*SEQ\(., ., .\).*' | wc -l"
    # assert_equal "$count" "0"
    # refute_output
  done;
}

@test 'flushed events receieved by fallback node()' {
  NON_FALLBACK_NODES=(0 4)
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
      echo $id | xargs -I {} rg "{}" ${LOG_DIR}/${FALLBACK_NODE}.log > /dev/null

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
