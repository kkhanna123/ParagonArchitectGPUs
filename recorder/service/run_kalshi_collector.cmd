@echo off
REM Permanent Kalshi collector: records every family in recorder\kalshi\adapters.py (15-minute
REM commodities + BTC, daily commodity ladders, city high/low temperatures, fed rates, US gas)
REM until the machine stops. Restart loop as in run_collector.cmd; the recorder takes a lock on
REM its output directory, so a second copy exits instead of duplicating the feed.
REM
REM Logs: %LOGDIR%\kalshi-<date>.log, appended. Locations and the API key file: paths.cmd.

setlocal
call "%~dp0paths.cmd"

:loop
for /f "tokens=2-4 delims=/- " %%a in ("%DATE%") do set TODAY=%%c-%%a-%%b
set LOG=%LOGDIR%\kalshi-%TODAY%.log
echo [%DATE% %TIME%] starting kalshi collector >> "%LOG%"
"%PY%" -m recorder.kalshi.recorder >> "%LOG%" 2>&1
echo [%DATE% %TIME%] kalshi collector exited with code %ERRORLEVEL%; restarting in 30s >> "%LOG%"
timeout /t 30 /nobreak >nul
goto loop
