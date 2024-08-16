#!/usr/bin/env bash

usage () {
  echo "Usage: run [OPTIONS] INEQUALITY_INPUTS_PATH";
  echo "";
  echo "Options:";
  echo "  -s: Flag to apply the adaptive strategy";
}

# Process options
while getopts ":s" opt; do
  case $opt in
    s)
      APPLY_STRATEGY=1
      echo "APPLY_STRATEGY was initi"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      usage
      exit 1
      ;;
  esac
done

# Shift off the options and optional --
shift $((OPTIND - 1))

if (( $# < 1 )); then
  usage
  exit 1
else
  INEQUALITY_INPUTS_PATH="$(realpath $1)"
fi;

echo "INEQUALITY_INPUTS_PATH = $INEQUALITY_INPUTS_PATH"

rm -f monitor*.log
rm -f totalRates*.csv

if [ -z $APPLY_STRATEGY ]; then
  echo "APPLY_STRATEGY not set"
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
  done;
else
  echo "APPLY_STRATEGY $APPLY_STRATEGY"
  for i in {0..4}
  do
    echo "Starting monitor $i"
      java -ea \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        -Dlog4j.configurationFile=log4j2.xml \
        -Dlogfile.name="monitor${i}.log" \
        -jar target/monitoring-0.1.jar \
        -applyStrategy \
        -node $i \
        -monitoringinputs $INEQUALITY_INPUTS_PATH &
  done;
fi
