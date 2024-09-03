#!/usr/bin/env bash

for i in $(ls /Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation | grep "^SEQ_")
do
  echo "/Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation/$i/plans"
	poetry run python generate_matrices.py --input_dir /Users/krispian/Uni/bachelorarbeit/topologies_delayed_systematic_inflation/$i/plans
done
