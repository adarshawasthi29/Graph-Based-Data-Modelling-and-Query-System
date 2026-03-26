# Run from repo root or from backend/:  .\backend\run.ps1
Set-Location $PSScriptRoot
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
