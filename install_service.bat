@echo off
set SERVICE_NAME=PyJobScheduler
set PYTHON_PATH="C:\Python39\python.exe"
set SCRIPT_PATH="%~dp0scheduler\scheduler.py"

nssm install %SERVICE_NAME% %PYTHON_PATH% %SCRIPT_PATH%
nssm set %SERVICE_NAME% AppDirectory "%~dp0"
nssm set %SERVICE_NAME% AppStdout "%~dp0logs\service.log"
nssm set %SERVICE_NAME% AppStderr "%~dp0logs\service_error.log"
nssm start %SERVICE_NAME%

echo Service %SERVICE_NAME% installiert und gestartet
pause