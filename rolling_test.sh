#!/usr/bin/env bash

TOPOLOGY_BASE_DIR=/Users/krispian/Uni/bachelorarbeit/topologies_delayed_inflation

run() {
  # ####################
  # #### NO STRATEGY ####
  # ####################
  # # start monitors
  # cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module/monitoring
  # ./start_monitors.sh "${INPUT_DIR}/inequality_inputs.json" $OUTPUT_DIR_NO_STRATEGY
  # sleep 15
  #
  # # start flink job
  # cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying
  # ./run_local $INPUT_DIR $OUTPUT_DIR_NO_STRATEGY $TRACE_DIR $NODE_N
  # sleep 10
  #
  # ####################
  # #### STRATEGY ####
  # ####################
  # # start monitors
  # cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module/monitoring
  # ./start_monitors.sh -s "${INPUT_DIR}/inequality_inputs.json" $OUTPUT_DIR_STRATEGY
  # sleep 5
  #
  # # start coordinator
  # cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/mama-module
  # java -ea \
  #     --add-opens java.base/java.util=ALL-UNNAMED \
  #     --add-opens java.base/java.lang=ALL-UNNAMED \
  #     -jar coordinator/target/coordinator-0.1.jar \
  #     -addressBook /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying/address_book_localhost.json \
  #     -n $NODE_N &
  #
  # # start flink job
  # cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/deploying
  # ./run_local $INPUT_DIR $OUTPUT_DIR_STRATEGY $TRACE_DIR $NODE_N
  # sleep 10
  #
  # # plot transmission rates
  cd /Users/krispian/Uni/bachelorarbeit/sigmod24-flink/python
  poetry run python plot_all_sent.py \
      --dir0 $OUTPUT_DIR_NO_STRATEGY \
      --dir1 $OUTPUT_DIR_STRATEGY \
      --output_dir $INPUT_DIR \
      --query $QUERY \
      --node_n $NODE_N \
      --events "A;B;C;${OP}(A, B)"

  poetry run python plot_sent_by_event.py \
      --dir0 $OUTPUT_DIR_NO_STRATEGY \
      --dir1 $OUTPUT_DIR_STRATEGY \
      --output_dir $INPUT_DIR \
      --query $QUERY \
      --node_n $NODE_N \
      --events "A;B;C;${OP}(A, B)"
}

for i in $(ls $TOPOLOGY_BASE_DIR)
do
  echo $i
  OP=$(echo $i | awk -F_ '{print $1}')
  QUERY=$(echo $i | awk -F_ '{print $1"("$2")"}')
  NODE_N=$(echo $i | awk '{n=split($1,A,"_"); print A[n]}')
  echo $NODE_N

  TOPOLOGY_PATH="${TOPOLOGY_BASE_DIR}/${i}"
  INPUT_DIR="${TOPOLOGY_PATH}/plans"
  TRACE_DIR="${INPUT_DIR}/trace_inflated_mix"
  OUTPUT_DIR_NO_STRATEGY="${INPUT_DIR}/output_strategy_0"
  OUTPUT_DIR_STRATEGY="${INPUT_DIR}/output_strategy_1"
  run
done
