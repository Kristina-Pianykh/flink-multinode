#!/usr/bin/env bash

TOPOLOGY_BASE_DIR=/Users/krispian/Uni/bachelorarbeit/topologies_test

run() {
  ####################
  #### NO STRATEGY ####
  ####################
  # start monitors
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module/monitoring
  ./start_monitors.sh "${INPUT_DIR}/inequality_inputs.json" $OUTPUT_DIR_NO_STRATEGY
  sleep 5

  # start flink job
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying
  ./run_local $INPUT_DIR $OUTPUT_DIR_NO_STRATEGY "${INPUT_DIR}/trace_inflated_1" $i
  sleep 10

  ####################
  #### STRATEGY ####
  ####################
  # start monitors
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module/monitoring
  ./start_monitors.sh -s "${INPUT_DIR}/inequality_inputs.json" $OUTPUT_DIR_STRATEGY
  sleep 5

  # start coordinator
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module
  java -ea \
      --add-opens java.base/java.util=ALL-UNNAMED \
      --add-opens java.base/java.lang=ALL-UNNAMED \
      -jar coordinator/target/coordinator-0.1.jar \
      -addressBook /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json \
      -n $i

  # start flink job
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying
  ./run_local $INPUT_DIR $OUTPUT_DIR_STRATEGY "${INPUT_DIR}/trace_inflated_1" $i
  sleep 10

  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/pyplt
  poetry run python plot_all_sent.py \
      --dir0 $OUTPUT_DIR_NO_STRATEGY \
      --dir1 $OUTPUT_DIR_STRATEGY \
      --output_dir $INPUT_DIR --events "A;B;C;SEQ(A, B)"
}


for i in {5..9}
do
  TOPOLOGY_PATH="${TOPOLOGY_BASE_DIR}/SEQ_ABC_${i}"
  INPUT_DIR="${TOPOLOGY_PATH}/plans"
  OUTPUT_DIR_NO_STRATEGY="${INPUT_DIR}/output_strategy_0"
  OUTPUT_DIR_STRATEGY="${INPUT_DIR}/output_strategy_1"

  echo "TOPOLOGY_PATH: $TOPOLOGY_PATH"
  echo "INPUT_DIR: $INPUT_DIR"
  echo "OUTPUT_DIR_NO_STRATEGY: $OUTPUT_DIR_NO_STRATEGY"
  echo "OUTPUT_DIR_STRATEGY: $OUTPUT_DIR_STRATEGY"
  run

done

i=5
TOPOLOGY_PATH="${TOPOLOGY_BASE_DIR}/AND_ABC_${i}"
INPUT_DIR="${TOPOLOGY_PATH}/plans"
OUTPUT_DIR_NO_STRATEGY="${INPUT_DIR}/output_strategy_0"
OUTPUT_DIR_STRATEGY="${INPUT_DIR}/output_strategy_1"
run


TOPOLOGY_PATH="${TOPOLOGY_BASE_DIR}/SEQ_ABC_complex_${i}"
INPUT_DIR="${TOPOLOGY_PATH}/plans"
OUTPUT_DIR_NO_STRATEGY="${INPUT_DIR}/output_strategy_0"
OUTPUT_DIR_STRATEGY="${INPUT_DIR}/output_strategy_1"

echo "TOPOLOGY_PATH: $TOPOLOGY_PATH"
echo "INPUT_DIR: $INPUT_DIR"
echo "OUTPUT_DIR_NO_STRATEGY: $OUTPUT_DIR_NO_STRATEGY"
echo "OUTPUT_DIR_STRATEGY: $OUTPUT_DIR_STRATEGY"
run
