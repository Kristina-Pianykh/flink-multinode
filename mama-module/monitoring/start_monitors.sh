#!/usr/bin/env bash

BASE_DIR=$(realpath "$(dirname "$0")")
echo "BASE_DIR: $BASE_DIR"

usage () {
  echo "Usage: run [OPTIONS] INEQUALITY_INPUTS_PATH OUTPUT_DIR";
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

if (( $# < 2 )); then
  usage
  exit 1
else
  INEQUALITY_INPUTS_PATH="$(realpath $1)"
  output_dir="$2"

  if [ ! -d "$2" ]; then
    echo "OUTPUT_DIR does not exist. Creating..."
    mkdir -p "$2"
  fi;

  OUTPUT_DIR=$2
  rm -f $OUTPUT_DIR/monitor*.log
  rm -f $OUTPUT_DIR/totalRates*.csv
fi;

echo "INEQUALITY_INPUTS_PATH = $INEQUALITY_INPUTS_PATH"
echo "OUTPUT_DIR = $OUTPUT_DIR"

if [ -z $APPLY_STRATEGY ]; then
  echo "APPLY_STRATEGY not set"
  for i in {0..19}
  do
    echo "Starting monitor $i"
      java -ea \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        -Dlog4j.configurationFile="$BASE_DIR/src/main/resources/log4j2.xml" \
        -Dlogfile.name="$OUTPUT_DIR/monitor${i}.log" \
        -jar target/monitoring-0.1.jar \
        -node $i \
        -monitoringinputs $INEQUALITY_INPUTS_PATH \
        -outputdir $OUTPUT_DIR &
  done;
else
  echo "APPLY_STRATEGY $APPLY_STRATEGY"
  for i in {0..19}
  do
    echo "Starting monitor $i"
      java -ea \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        -Dlog4j.configurationFile="$BASE_DIR/src/main/resources/log4j2.xml" \
        -Dlogfile.name="$OUTPUT_DIR/monitor${i}.log" \
        -jar target/monitoring-0.1.jar \
        -applyStrategy \
        -node $i \
        -monitoringinputs $INEQUALITY_INPUTS_PATH \
        -outputdir $OUTPUT_DIR &
  done;
fi
