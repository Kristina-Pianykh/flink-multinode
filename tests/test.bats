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
