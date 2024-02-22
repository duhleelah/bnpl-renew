@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from run_hypertune_neuralnet.txt
for /f "tokens=*" %%a in (run_hypertune_neuralnet.txt) do (
  %%a
)

endlocal