@echo off
chcp 866 >nul
cd /d "%~dp0"

REM --- Yavno lokalny rezhim (perebivaet sistemnye peremenye) ---
set IS_RENDER=False

REM --- Uivaem starye protsessy Python/uvicorn na portu 8000 ---
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8000 ^| findstr LISTENING 2^>nul') do (
    echo Killing old process PID: %%a on port 8000...
    taskkill /F /PID %%a 2>nul
)
timeout /t 1 /nobreak >nul

REM --- Virtualnoe okruzhenie ---
if not exist .venv (
    echo Creating venv...
    python -m venv .venv
    call .venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate.bat
)

REM --- Proverka .env ---
if not exist .env (
    echo.
    echo [WARN] Fail .env ne naiden!
    echo Sozdaem .env dlya lokalnogo SQLite...
    (
        echo # Bybit API (neobyazatelno, dlya torgovli)
        echo BYBIT_API_KEY=your_api_key
        echo BYBIT_API_SECRET=your_api_secret
        echo.
        echo # Lokalnaya baza dannyh (SQLite)
        echo DATABASE_URL=sqlite+aiosqlite:///data/knife.db
        echo.
        echo # Rezhim zapuska (False dlya PK)
        echo IS_RENDER=False
    ) > .env
    echo [OK] .env sozdan.
)

REM --- Zapusk (avtomaticheskiy restart pri avanii) ---
echo Starting Knife Bot locally (IS_RENDER=%IS_RENDER%)...
:restart_loop
.venv\Scripts\uvicorn.exe app.main:app --host 127.0.0.1 --port 8000

REM --- Ctrl+C = osoznannaya ostanovka, bez restarta i podtverzhdeniy ---
if errorlevel 130 (
    echo.
    echo [OK] Bot stopped by user. Exiting without restart.
    exit /b 0
)

echo.
echo [!] Bot exited ^(crashed or stopped^). Restarting in 2 seconds...
timeout /t 2 >nul
goto :restart_loop
