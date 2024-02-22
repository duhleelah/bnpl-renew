#!/bin/bash

# Read and execute commands from run_files_2.txt
while read -r line; do
  $line
done < run_files_2.txt