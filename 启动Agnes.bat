@echo off
cd /d "%~dp0"
set PYTHONUTF8=1
start http://localhost:8766
uvicorn main:app --host 0.0.0.0 --port 8766
