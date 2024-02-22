from .constants import *
from .make_partitioned_scripts import *
import platform


# Constants specifically for making the whole run script
# NOTE: Add more files here to run, example impute/business rule checks
REMOVE_PY_VAL = -3
LINUX = "Linux"
WINDOWS = "Windows"
MAC = "Mac"
DARWIN = "Darwin"

PYTHON_CONSOLE_FORMAT = "python3 -m scripts.{}"

INTERNAL_ETL = ["etl", "internal_joins"]
INTERNAL_ETL_2 = ["etl_2", "internal_joins"]
RUN_FIRST = [
    "external_etl",
    "make_partitioned_scripts",
]

# Delete patterns for windows and linux/mac files
WINDOWS_DELETE_PATTERN = "del *{}* /s"
LINUX_DELETE_PATTERN = """find -type f -name '*{}*' -delete"""
MAC_DELETE_PATTERN = """find . -name '*{}*' -delete"""


# Write over this constant once we have all the files we need
AFTER_JOIN_ORDER = [

]

AGE_ALLOCATION = "sa2_age_allocation"
    
# Batch and Shell File Formats
BATCH_FORMAT = """@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from {}
for /f "tokens=*" %%a in ({}) do (
  %%a
)

endlocal"""

SHELL_FORMAT = """#!/bin/bash

# Read and execute commands from {}
while read -r line; do
  echo "$line"
  eval "$line"
done < {}"""

def write_run_file(folder:str=TABLES_DIR) -> None:
    """
    Writes the run file used to run the ETL
    - Parameters
        - folder: Folder used to determine the length of transactions data files
    - Returns
        - None, but saves the needed files to run the ETL in one shell/batch file
    """
    transactions_length = get_transactions_length(folder)
    # Extract the needed join scripts with transactions
    transactions_raw_partitions = [TRANSACTIONS_RAW_NAME_FORMAT.format(i//TRANSACTIONS_RAW_NUM_FILES)[:REMOVE_PY_VAL]\
                                                                    for i in range(0, 
                                                                    transactions_length,
                                                                    TRANSACTIONS_RAW_NUM_FILES)]
    transactions_transformed_partitions = [TRANSACTIONS_TRANSFORM_NAME_FORMAT.format(i//TRANSACTIONS_TRANSFORM_NUM_FILES)[:REMOVE_PY_VAL] \
                                                                    for i in range(0, 
                                                                    transactions_length,
                                                                    TRANSACTIONS_TRANSFORM_NUM_FILES)]

    # Now make the complete run file
    run_file_text = []
    delete_consumers = None
    delete_merchants = None
    delete_raw_partitions = None
    delete_transformed_partitions = None
    delete_whole_partitions = None
    delete_ohe_small_partitions = None
    delete_distinct_partitions = None

    # Need to create the run commands based on the computer's platform
    if platform.system() == LINUX:
        delete_consumers = LINUX_DELETE_PATTERN.format(TRANSACTIONS_CONSUMERS)
        delete_merchants = LINUX_DELETE_PATTERN.format(TRANSACTIONS_MERCHANTS)
        delete_raw_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_RAW_PARTITION)
        delete_transformed_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_TRANSFORMED_PARTITION)
        delete_whole_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_WHOLE)
        delete_ohe_small_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_OHE_SMALL)
        delete_distinct_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_DISTINCT)
        delete_batch_partitions = LINUX_DELETE_PATTERN.format(TRANSACTIONS_BATCH)
    elif platform.system() == WINDOWS:
        delete_consumers = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_CONSUMERS)
        delete_merchants = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_MERCHANTS)
        delete_raw_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_RAW_PARTITION)
        delete_transformed_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_TRANSFORMED_PARTITION)
        delete_whole_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_WHOLE)
        delete_ohe_small_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_OHE_SMALL)
        delete_distinct_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_DISTINCT)
        delete_batch_partitions = WINDOWS_DELETE_PATTERN.format(TRANSACTIONS_BATCH)
    else:
        delete_consumers = MAC_DELETE_PATTERN.format(TRANSACTIONS_CONSUMERS)
        delete_merchants = MAC_DELETE_PATTERN.format(TRANSACTIONS_MERCHANTS)
        delete_raw_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_RAW_PARTITION)
        delete_transformed_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_TRANSFORMED_PARTITION)
        delete_whole_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_WHOLE)
        delete_ohe_small_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_OHE_SMALL)
        delete_distinct_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_DISTINCT)
        delete_batch_partitions = MAC_DELETE_PATTERN.format(TRANSACTIONS_BATCH)

    # Now write the whole text file to store the console commands
    run_file_text = [] + RUN_FIRST + INTERNAL_ETL + transactions_raw_partitions + transactions_transformed_partitions + [AGE_ALLOCATION]
    delete_lines = [delete_consumers, delete_merchants, delete_raw_partitions, delete_transformed_partitions, \
                    delete_whole_partitions, delete_distinct_partitions, delete_ohe_small_partitions, delete_batch_partitions] #, delete_ohe_big_partitions]
    run_file_text = [PYTHON_CONSOLE_FORMAT.format(fname) for fname in run_file_text] + delete_lines
    run_file_text = "\n".join(run_file_text)+"\n"

    # Save the text file along with shell and batch files to run the console commands
    with open(RUN_SCRIPT_TXT_PATH, "w") as fp:
        fp.write(run_file_text)
    with open(RUN_SCRIPT_SH_PATH, "w") as fp:
        fp.write(SHELL_FORMAT.format(RUN_FILES_TXT, RUN_FILES_TXT))
    with open(RUN_SCRIPT_BATCH_PATH, "w") as fp:
        fp.write(BATCH_FORMAT.format(RUN_FILES_TXT, RUN_FILES_TXT))

if __name__ == "__main__":
    write_run_file()