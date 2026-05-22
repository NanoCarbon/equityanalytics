@echo off
setlocal EnableDelayedExpansion
title Equity Analytics
color 1F

cd /d "%~dp0"

:: ─────────────────────────────────────────────────────────────────────────────
::  EQUITY ANALYTICS
::  1) Ensure Docker containers are up
::  2) Wait for Airflow to be healthy
::  3) Run db_health_check and display summary in terminal
::  4) Launch Streamlit app on http://localhost:8501
::     (DB Health tab available inside the app under "04 . DB Health")
:: ─────────────────────────────────────────────────────────────────────────────

echo.
echo ============================================================
echo   EQUITY ANALYTICS
echo ============================================================
echo.

:: ── Pre-flight: verify Python and Streamlit are available ─────────────────────
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: python not found in PATH.
    echo Make sure Python 3.11 is installed and available.
    goto FAIL
)

python -m streamlit --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: streamlit not installed for this Python.
    echo Run:  python -m pip install streamlit
    goto FAIL
)

:: ── 1. Docker Compose up ─────────────────────────────────────────────────────
echo [1/4] Starting Docker containers...
docker compose up -d
if %ERRORLEVEL% NEQ 0 (
    echo   WARNING: docker compose up returned non-zero. Containers may already be running.
) else (
    echo   Containers started ^(or already running^).
)
echo.

:: ── 2. Wait for Airflow webserver ────────────────────────────────────────────
echo [2/4] Waiting for Airflow to be ready...
set /a TRIES=0
set /a MAX_TRIES=24

:WAIT_LOOP
set /a TRIES+=1
curl -s -o nul -w "%%{http_code}" http://localhost:8080/health 2>nul | findstr "200" >nul
if %ERRORLEVEL% EQU 0 (
    echo   Airflow is healthy.
    goto AIRFLOW_READY
)
if %TRIES% GEQ %MAX_TRIES% (
    echo   WARNING: Airflow did not respond after %MAX_TRIES% attempts.
    echo   The app will still open. Airflow UI at http://localhost:8080 may need more time.
    goto AIRFLOW_READY
)
echo   Attempt %TRIES%/%MAX_TRIES% - waiting 5s...
timeout /t 5 /nobreak >nul
goto WAIT_LOOP

:AIRFLOW_READY

:: ── 3. DB Health Check ───────────────────────────────────────────────────────
echo.
echo [3/4] Running database health check...
echo ============================================================
python scripts\db_health_check.py
set HEALTH_EXIT=%ERRORLEVEL%
echo ============================================================
if %HEALTH_EXIT% EQU 0 (
    echo   Health check: PASS
) else (
    echo   Health check: ISSUES FOUND ^(exit %HEALTH_EXIT%^) - see above for details
)

:: ── 4. Launch Streamlit ──────────────────────────────────────────────────────
echo.
echo [4/4] Launching Equity Analytics on http://localhost:8501 ...
echo   DB Health tab:  "04 . DB Health" inside the app
echo   Airflow UI:     http://localhost:8080
echo   Close this window to stop the app.
echo.

python -m streamlit run app\streamlit_app.py ^
    --server.port 8501 ^
    --server.headless false ^
    --browser.gatherUsageStats false ^
    --theme.base light

:: ── If Streamlit exits, pause so the user can read any error output ───────────
echo.
echo Streamlit has stopped.
goto END

:FAIL
echo.
echo Startup failed - check the errors above.

:END
echo.
pause
endlocal
