@echo off
rem Starts the Hyperliquid recorder. Arguments pass through, e.g.
rem   record.bat --dex xyz --coins CL,GOLD,SP500
cd /d "%~dp0\..\.."
if not exist .venv\Scripts\python.exe (
    echo Run recorder\windows\setup.bat first.
    pause
    exit /b 1
)
.venv\Scripts\python.exe -m recorder.hl_recorder %*
