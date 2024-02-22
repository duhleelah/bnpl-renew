@echo off
setlocal enabledelayedexpansion

REM Read and execute commands from run_files_2.txt
for /f "tokens=*" %%a in (run_files_2.txt) do (
  %%a
)

endlocal