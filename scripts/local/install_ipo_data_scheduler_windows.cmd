@echo off
setlocal

set "TASK_NAME=PublicOfferingSharesIpoDataScheduler"
if not "%IPO_DATA_WINDOWS_TASK_NAME%"=="" set "TASK_NAME=%IPO_DATA_WINDOWS_TASK_NAME%"

set "INTERVAL_MINUTES=%~1"
if "%INTERVAL_MINUTES%"=="" set "INTERVAL_MINUTES=10"

set "ROOT_DIR=%~dp0..\.."
for %%I in ("%ROOT_DIR%") do set "ROOT_DIR=%%~fI"
set "BUILD_DIR=%ROOT_DIR%\build\local_scheduler"
set "LOG_DIR=%BUILD_DIR%\logs"
set "RUNNER_CMD=%BUILD_DIR%\ipo-data-scheduler-tick.cmd"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "usebackq delims=" %%I in (`wsl.exe wslpath -u "%ROOT_DIR%"`) do set "WSL_ROOT_DIR=%%I"

> "%RUNNER_CMD%" echo @echo off
>> "%RUNNER_CMD%" echo wsl.exe -e bash -lc "cd '%WSL_ROOT_DIR%' ^&^& scripts/local/ipo_data_scheduler_tick.sh" ^>^> "%LOG_DIR%\ipo-data-scheduler.out.log" 2^>^> "%LOG_DIR%\ipo-data-scheduler.err.log"

schtasks /Create /SC MINUTE /MO %INTERVAL_MINUTES% /TN %TASK_NAME% /TR "%RUNNER_CMD%" /F
if errorlevel 1 exit /b %errorlevel%

echo Installed %TASK_NAME%
echo Interval: %INTERVAL_MINUTES%m
echo Runner: %RUNNER_CMD%
echo Logs: %LOG_DIR%
echo Run now: schtasks /Run /TN %TASK_NAME%
