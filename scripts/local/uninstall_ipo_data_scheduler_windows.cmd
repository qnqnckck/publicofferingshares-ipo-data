@echo off
setlocal

set "TASK_NAME=PublicOfferingSharesIpoDataScheduler"
if not "%IPO_DATA_WINDOWS_TASK_NAME%"=="" set "TASK_NAME=%IPO_DATA_WINDOWS_TASK_NAME%"

schtasks /Delete /TN %TASK_NAME% /F
