#!/usr/bin/env bash

BASE_DIR=$(realpath "$(dirname "$0")")
echo "BASE_DIR: $BASE_DIR"

TOPOLOGY_DIR="/Users/krispian/Uni/bachelorarbeit/topologies_delayed_inflation"

for i in $(ls $TOPOLOGY_DIR)
do
  poetry run python ${BASE_DIR}/mix_traces.py \
      --topology_dir "${TOPOLOGY_DIR}/${i}"
  if [ $? -ne 0 ]; then
    echo "Error mixing traces for ${TOPOLOGY_DIR}/${i}"
    exit 1
  fi
done
