@echo off
REM One-shot daily upkeep. Single definition of the steps lives in daily_runner.py; this
REM wrapper exists for manual runs and for the Task Scheduler path in install_tasks.ps1.
setlocal
call "%~dp0paths.cmd"
if "%PARAGON_RAW_KEEP_DAYS%"=="" set PARAGON_RAW_KEEP_DAYS=30
"%PY%" -m recorder.service.daily_runner --once --keep-days %PARAGON_RAW_KEEP_DAYS% --log-dir "%LOGDIR%"
