@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from delete_partitions.txt
for /f "tokens=*" %%a in (delete_partitions.txt) do (
  %%a
)

endlocal