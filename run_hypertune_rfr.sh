#!/bin/bash

# Read and execute commands from run_hypertune_rfr.txt
while read -r line; do
  echo "$line"
  eval "$line"
done < run_hypertune_rfr.txt