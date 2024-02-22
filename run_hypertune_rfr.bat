@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from run_hypertune_rfr.txt
for /f "tokens=*" %%a in (run_hypertune_rfr.txt) do (
  %%a
)

endlocal