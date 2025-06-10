@echo off

echo ================================
echo   SIMPLE CI CHECK
echo ================================

echo Step 1: Check if code works...
python -m dbt.cli.main run
if %errorlevel% neq 0 (
    echo FAILED! Your code has problems.
    exit /b 1
)

echo Step 2: Check if tests pass...
python -m dbt.cli.main test
if %errorlevel% neq 0 (
    echo FAILED! Your tests failed.
    exit /b 1
)

echo.
echo SUCCESS! Your code is good to go!
echo.