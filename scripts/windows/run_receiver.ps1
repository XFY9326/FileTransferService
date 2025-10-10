$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $scriptDir
Write-Output "Current working dir: $scriptDir"

Write-Output "Launching script..."

uv run src/fts/receiver.py

Read-Host -Prompt "Press Enter to close"
