@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from run_hypertune_ts.txt
for /f "tokens=*" %%a in (run_hypertune_ts.txt) do (
  %%a
)

endlocal