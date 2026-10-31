' Launches run_collector.cmd with no visible console window.
'
' This is the unelevated autostart path: Task Scheduler registration is denied on this machine
' (schtasks and Register-ScheduledTask both return Access denied, even for a per-user logon
' task), so a copy of this script goes in the per-user Startup folder instead.
'
' Trade-off versus a scheduled task: this runs at *logon*, not at boot, and so does not collect
' while the machine sits at the lock screen after a reboot with nobody logged in. The restart
' loop inside run_collector.cmd still covers the recorder crashing. If the task-scheduler
' restriction is ever lifted, recorder\service\install_tasks.ps1 registers the better version
' (boot trigger, restart-on-failure, runs logged out) in one command from an elevated shell.
'
' Starting a second collector is harmless: the recorder takes an exclusive lock on its output
' directory and exits with a clear message rather than duplicating the feed.

Dim shell, fso, here, target
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
here = fso.GetParentFolderName(WScript.ScriptFullName)
target = fso.BuildPath(here, "run_collector.cmd")

If Not fso.FileExists(target) Then
    MsgBox "Paragon collector not found at " & target, 16, "Paragon collector"
    WScript.Quit 1
End If

' 0 = hidden window, False = do not wait for it to finish
shell.Run """" & target & """", 0, False
