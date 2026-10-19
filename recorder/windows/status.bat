@echo off
cd /d "%~dp0\..\.."
.venv\Scripts\python.exe -m recorder.hl_check --status
pause
