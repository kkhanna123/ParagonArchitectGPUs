<#
Registers the two scheduled tasks that make the collector permanent.

NOTE (2026-10-04): task registration is currently DENIED on this machine. Both
`Register-ScheduledTask` and `schtasks /create` return "Access is denied" even unelevated for a
per-user logon task, which points at policy rather than just missing elevation. The collector
therefore runs from the per-user Startup folder instead -- see recorder\service\README.md.
Try this script again from an elevated shell if that restriction is ever lifted; it gives a
strictly better setup (boot trigger, restart-on-failure, runs while logged out).

  ParagonXYZCollector   runs run_collector.cmd at boot and keeps it running
  ParagonXYZMaintenance runs daily_maintenance.cmd once a day at 00:40 UTC-ish local

Both run as the current user. The collector needs no elevation and no network credentials
(Hyperliquid market data is public), so it deliberately does NOT run as SYSTEM: that would
lose the user profile the venv and PARAGON_DATA_ROOT are resolved against.

  powershell -ExecutionPolicy Bypass -File recorder\service\install_tasks.ps1
  powershell -ExecutionPolicy Bypass -File recorder\service\install_tasks.ps1 -Uninstall

-AtLogon registers the collector to start at logon instead of boot, which is the right choice
if the account is not set to run tasks while logged out.
#>
param(
    [switch]$Uninstall,
    [switch]$AtLogon,
    [string]$DataRoot = $env:PARAGON_DATA_ROOT,
    [int]$KeepDays = 30
)

$ErrorActionPreference = "Stop"
$CollectorTask = "ParagonXYZCollector"
$MaintenanceTask = "ParagonXYZMaintenance"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Uninstall) {
    foreach ($n in @($CollectorTask, $MaintenanceTask)) {
        if (Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue) {
            Stop-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue
            Unregister-ScheduledTask -TaskName $n -Confirm:$false
            "unregistered $n"
        } else { "$n was not registered" }
    }
    exit 0
}

if (-not $DataRoot) { $DataRoot = "C:\ParagonData" }
if ($DataRoot -like "*OneDrive*") {
    throw "DataRoot $DataRoot is inside OneDrive; pick a path outside it (see Data/tradexyz/storage.md)"
}
$py = Join-Path (Resolve-Path (Join-Path $here "..\..")) ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "venv python not found at $py; create it first" }

# The data root is set machine-wide rather than passed as an argument, so an interactive
# `python -m recorder.hl_db --report` in a fresh shell reads the same tree the tasks write.
[Environment]::SetEnvironmentVariable("PARAGON_DATA_ROOT", $DataRoot, "User")
$env:PARAGON_DATA_ROOT = $DataRoot
"PARAGON_DATA_ROOT = $DataRoot (persisted for this user)"

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Limited

$elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $elevated) {
    Write-Warning "Not elevated. Registering an at-startup task needs admin, and on this machine registration is denied outright. If this fails, use the Startup-folder setup in recorder\service\README.md."
}

# --- collector: start on boot/logon, restart if it ever stops, never time out -------------
$collectorTrigger = if ($AtLogon) { New-ScheduledTaskTrigger -AtLogOn } else { New-ScheduledTaskTrigger -AtStartup }
$collectorSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -DontStopOnIdleEnd `
    -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999 `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$collectorAction = New-ScheduledTaskAction -Execute "cmd.exe" `
    -Argument ('/c "' + (Join-Path $here "run_collector.cmd") + '"') `
    -WorkingDirectory (Resolve-Path (Join-Path $here "..\.."))
Register-ScheduledTask -TaskName $CollectorTask -Trigger $collectorTrigger -Action $collectorAction `
    -Principal $principal -Settings $collectorSettings -Force `
    -Description "Records trade[XYZ] L2 books (core watchlist) continuously. 0.5s depth is live-only and cannot be backfilled." | Out-Null
"registered $CollectorTask (trigger: $(if ($AtLogon) {'logon'} else {'boot'}))"

# --- maintenance: convert, backfill, refresh catalog, prune raw ---------------------------
$maintTrigger = New-ScheduledTaskTrigger -Daily -At "00:40"
$maintSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$maintAction = New-ScheduledTaskAction -Execute "cmd.exe" `
    -Argument ('/c "' + (Join-Path $here "daily_maintenance.cmd") + '"') `
    -WorkingDirectory (Resolve-Path (Join-Path $here "..\.."))
Register-ScheduledTask -TaskName $MaintenanceTask -Trigger $maintTrigger -Action $maintAction `
    -Principal $principal -Settings $maintSettings -Force `
    -Description "Daily: convert raw to parquet, re-backfill candles/funding (the 1m window is only ~3.5 days deep), refresh the DuckDB catalog, prune raw older than $KeepDays days." | Out-Null
"registered $MaintenanceTask (daily 00:40 local)"

""
"Start the collector now with:  Start-ScheduledTask -TaskName $CollectorTask"
"Check it with:                 Get-ScheduledTask -TaskName $CollectorTask | Get-ScheduledTaskInfo"
"Logs:                          $DataRoot\logs\"
