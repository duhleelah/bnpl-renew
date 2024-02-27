#!/bin/bash

# Read and execute commands from run_files.txt
while read -r line; do
  echo "$line"
  eval "$line"
done < run_files.txt