@echo off
REM Permanent trade[XYZ] collector: records the `all` watchlist (core + every commodity + the
REM research list, see hl_common.WATCHLISTS) until the machine stops.
REM
REM Wrapped in a restart loop as well as being a Task Scheduler job with restart-on-failure,
REM because the two cover different failures: the loop handles the recorder exiting (a crash
REM it could not reconnect through), the task handles reboots and the loop itself dying.
REM
REM Logs: %LOGDIR%\collector-<date>.log, appended. Locations: paths.cmd.

setlocal
call "%~dp0paths.cmd"

:loop
for /f "tokens=2-4 delims=/- " %%a in ("%DATE%") do set TODAY=%%c-%%a-%%b
set LOG=%LOGDIR%\collector-%TODAY%.log
echo [%DATE% %TIME%] starting collector >> "%LOG%"
REM --connections 2 gives 2 shards per role = 6 sockets, leaving 4 of the 10-per-IP budget
REM free for ad-hoc queries and a second recorder without tripping the limit.
"%PY%" -m recorder.hl_xyz_recorder --watchlist all --connections 2 >> "%LOG%" 2>&1
echo [%DATE% %TIME%] collector exited with code %ERRORLEVEL%; restarting in 30s >> "%LOG%"
timeout /t 30 /nobreak >nul
goto loop
