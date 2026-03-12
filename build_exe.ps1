param(
    [string]$Name = "PharmaAnalytics"
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

python -m pip install pyinstaller

python -m PyInstaller `
  --onefile `
  --collect-all streamlit `
  --name $Name `
  launcher.py

Write-Host ""
Write-Host "Build complete."
Write-Host "Executable:" (Join-Path $PSScriptRoot "dist\$Name.exe")
