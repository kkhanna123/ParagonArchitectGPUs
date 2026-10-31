@echo off
REM Shared locations for the service scripts. CALLed by each of them after `setlocal`.
REM
REM   PARAGON_DATA_ROOT  finished data: parquet + hl.duckdb (OneDrive-synced, written daily)
REM   PARAGON_RAW_ROOT   raw JSONL staging, appended every few seconds -- kept OUT of OneDrive
REM   LOGDIR             logs and lock files, also outside OneDrive
REM
REM Any of these already set in the environment wins.
if "%PARAGON_DATA_ROOT%"=="" set "PARAGON_DATA_ROOT=%USERPROFILE%\OneDrive\Desktop\Paragon\HyperliquidData"
if "%PARAGON_RAW_ROOT%"=="" set "PARAGON_RAW_ROOT=C:\ParagonData\raw"
if "%LOGDIR%"=="" set "LOGDIR=C:\ParagonData\logs"
set "REPO=%~dp0..\.."
set "PY=%REPO%\.venv\Scripts\python.exe"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
if not exist "%PARAGON_DATA_ROOT%" mkdir "%PARAGON_DATA_ROOT%"
REM `python -m recorder.*` resolves the package from the working directory. Launched from the
REM Startup folder the working directory is not the repo, which is what broke collection on
REM 2026-10-14 (ModuleNotFoundError: No module named 'recorder' on every restart).
cd /d "%REPO%"
