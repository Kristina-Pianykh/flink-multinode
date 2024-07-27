#!/usr/bin/env bash

rm -f monitor*.log
rm -f totalRates*.log
for i in {0..4}
do
  if [ $i -ne 2 ] && [ $i -ne 3 ]; then
		java -ea --add-opens java.base/java.lang=ALL-UNNAMED -jar target/monitoring-0.1.jar -node $i -monitoringinputs "/Users/krispian/Uni/bachelorarbeit/test_flink_inputs_dev/generate_flink_inputs/plans/inequality_inputs.json" > monitor$i.log 2>&1 &
  fi
done
