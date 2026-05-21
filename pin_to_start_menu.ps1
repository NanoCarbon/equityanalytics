# pin_to_start_menu.ps1
# Creates a Start Menu shortcut for the Equity Analytics Admin Launcher.
# Run once from an elevated PowerShell prompt:
#   PowerShell -ExecutionPolicy Bypass -File pin_to_start_menu.ps1

$ErrorActionPreference = "Stop"

$repoRoot   = Split-Path -Parent $MyInvocation.MyCommand.Path
$batFile    = Join-Path $repoRoot "launch_admin.bat"
$iconPath   = Join-Path $repoRoot "admin_dashboard\styles\admin.ico"   # optional custom icon
$startMenu  = [Environment]::GetFolderPath("StartMenu")
$shortcutPath = Join-Path $startMenu "Programs\Equity Analytics Admin.lnk"

if (-not (Test-Path $batFile)) {
    Write-Error "launch_admin.bat not found at $batFile"
    exit 1
}

$wsh = New-Object -ComObject WScript.Shell
$sc  = $wsh.CreateShortcut($shortcutPath)
$sc.TargetPath       = "cmd.exe"
$sc.Arguments        = "/k `"$batFile`""
$sc.WorkingDirectory = $repoRoot
$sc.WindowStyle      = 1   # Normal window
$sc.Description      = "Equity Analytics Admin Dashboard"

# Use custom icon if it exists, otherwise fall back to cmd icon
if (Test-Path $iconPath) {
    $sc.IconLocation = $iconPath
} else {
    $sc.IconLocation = "C:\Windows\System32\cmd.exe,0"
}

$sc.Save()

Write-Host ""
Write-Host "Shortcut created: $shortcutPath"
Write-Host ""
Write-Host "To pin to Start, open the Start Menu, find 'Equity Analytics Admin',"
Write-Host "right-click it, and select 'Pin to Start'."
Write-Host ""
Write-Host "To pin to Taskbar, right-click the shortcut in Start and choose"
Write-Host "'More > Pin to taskbar'."
