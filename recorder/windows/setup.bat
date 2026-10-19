@echo off
rem One-time Windows setup: creates .venv at the repo root and installs requirements.txt.
cd /d "%~dp0\..\.."
where py >nul 2>nul && (set "PY=py -3") || (set "PY=python")
%PY% --version >nul 2>nul
if errorlevel 1 (
    echo Python not found. Install Python 3.12 from https://www.python.org/downloads/
    echo and tick "Add python.exe to PATH", then run this again.
    pause
    exit /b 1
)
if not exist .venv %PY% -m venv .venv || goto :fail
.venv\Scripts\python.exe -m pip install --upgrade pip --quiet || goto :fail
.venv\Scripts\python.exe -m pip install -r requirements.txt --quiet || goto :fail
echo.
echo Setup complete. Start recording with recorder\windows\record.bat
pause
exit /b 0
:fail
echo Setup failed - see the error above.
pause
exit /b 1
