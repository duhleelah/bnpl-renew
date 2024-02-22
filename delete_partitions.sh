#!/bin/bash

# Read and execute commands from delete_partitions.txt
while read -r line; do
  $line
done < delete_partitions.txt