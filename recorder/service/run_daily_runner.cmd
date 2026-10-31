@echo off
REM Keeps the daily-maintenance scheduler alive, mirroring run_collector.cmd. Needed because
REM Task Scheduler registration is denied on this machine.
setlocal
call "%~dp0paths.cmd"
:loop
echo [%DATE% %TIME%] starting daily runner >> "%LOGDIR%\daily_runner.log"
"%PY%" -m recorder.service.daily_runner --at 00:40 --keep-days 30 --log-dir "%LOGDIR%" >> "%LOGDIR%\daily_runner.log" 2>&1
echo [%DATE% %TIME%] daily runner exited %ERRORLEVEL%; restarting in 60s >> "%LOGDIR%\daily_runner.log"
timeout /t 60 /nobreak >nul
goto loop
