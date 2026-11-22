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

REM --- Zapusk bota v tekom okne bez avto-restarta ---
REM --- Ctrl+C ostanavlivaet uvicorn i srazu zahodit v komandnuyu stroku ---
echo Starting Knife Bot locally (IS_RENDER=%IS_RENDER%)...
start "KnifeBot" cmd /k "cd /d "%~dp0" && call .venv\Scripts\activate.bat && set IS_RENDER=False && uvicorn app.main:app --host 127.0.0.1 --port 8000 --no-use-colors"
exit /b 0
