@echo off
REM -----------------------------------------------
REM Activate venv from C:\Users\User1\datapull
REM Run migrate.py from current dir (oracle_to_pg)
REM Log output to migration_log.txt
REM -----------------------------------------------

REM Set log file path (in current directory)
set LOGFILE=%~dp0migration_log.txt

REM Log start time
echo ============================= >> "%LOGFILE%"
echo Run started at %date% %time% >> "%LOGFILE%"
echo ============================= >> "%LOGFILE%"

REM Step 1: Activate the virtual environment (absolute path)
echo Activating virtual environment... >> "%LOGFILE%"
call "%USERPROFILE%\datapull\Scripts\activate.bat" >> "%LOGFILE%" 2>&1

REM Step 2: Log active Python info
echo Checking active Python interpreter... >> "%LOGFILE%"
where python >> "%LOGFILE%" 2>&1
python --version >> "%LOGFILE%" 2>&1

REM Step 3: Run the migration script with arguments
echo Running migrate.py... >> "%LOGFILE%"
python migrate_pro.py gst_registration tables_to_migrate.csv >> "%LOGFILE%" 2>&1

REM Step 4: Confirm completion
echo Script execution completed at %date% %time% >> "%LOGFILE%"
echo. >> "%LOGFILE%"

REM Open the log file for review
notepad "%LOGFILE%"
