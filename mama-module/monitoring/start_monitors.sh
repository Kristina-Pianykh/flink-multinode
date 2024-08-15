#!/usr/bin/env bash

INEQUALITY_INPUTS_PATH="$(realpath $1)"
if [ -z "$INEQUALITY_INPUTS_PATH" ]; then
  echo "Usage: $0 <inequality_inputs_path>"
  exit 1
fi
echo "INEQUALITY_INPUTS_PATH = $INEQUALITY_INPUTS_PATH"

rm -f monitor*.log
rm -f totalRates*.log
for i in {0..4}
do
  # if [ $i -ne 1 ] ; then
		java -ea --add-opens java.base/java.lang=ALL-UNNAMED -jar target/monitoring-0.1.jar -node $i -monitoringinputs $INEQUALITY_INPUTS_PATH > monitor$i.log 2>&1 &
  # fi
done
