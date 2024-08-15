#!/usr/bin/env bash

INEQUALITY_INPUTS_PATH="$(realpath $1)"
if [ -z "$INEQUALITY_INPUTS_PATH" ]; then
  echo "Usage: $0 <inequality_inputs_path>"
  exit 1
fi
echo "INEQUALITY_INPUTS_PATH = $INEQUALITY_INPUTS_PATH"

rm -f monitor*.log
rm -f totalRates*.csv
for i in {0..4}
do
  echo "Starting monitor $i"
		java -ea \
      --add-opens java.base/java.lang=ALL-UNNAMED \
      -Dlog4j.configurationFile=log4j2.xml \
      -Dlogfile.name="monitor${i}.log" \
      -jar target/monitoring-0.1.jar \
      -node $i \
      -monitoringinputs $INEQUALITY_INPUTS_PATH &
done
