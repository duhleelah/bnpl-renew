@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from run_files.txt
for /f "tokens=*" %%a in (run_files.txt) do (
  %%a
)

endlocal