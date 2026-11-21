@echo off
cd /d "%~dp0"

REM --- Явно локальный режим (перебивает системные переменные) ---
set IS_RENDER=False

REM --- Убиваем старые процессы Python/uvicorn на порту 8000 ---
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8000 ^| findstr LISTENING 2^>nul') do (
    echo Killing old process PID: %%a on port 8000...
    taskkill /F /PID %%a 2>nul
)
timeout /t 1 /nobreak >nul

REM --- Виртуальное окружение ---
if not exist .venv (
    echo Creating venv...
    python -m venv .venv
    call .venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate.bat
)

REM --- Проверка .env ---
if not exist .env (
    echo.
    echo [WARN] Файл .env не найден!
    echo Создаём .env для локального SQLite...
    (
        echo # Bybit API (необязательно, для торговли)
        echo BYBIT_API_KEY=your_api_key
        echo BYBIT_API_SECRET=your_api_secret
        echo.
        echo # Локальная база данных (SQLite)
        echo DATABASE_URL=sqlite+aiosqlite:///data/knife.db
        echo.
        echo # Режим запуска (False для ПК)
        echo IS_RENDER=False
    ) > .env
    echo [OK] .env создан.
)

REM --- Запуск (автоматический рестарт при аварии) ---
echo Starting Knife Bot locally (IS_RENDER=%IS_RENDER%)...
:restart_loop
.venv\Scripts\uvicorn.exe app.main:app --host 127.0.0.1 --port 8000
echo.
echo [!] Bot exited (crashed or stopped). Restarting in 2 seconds...
timeout /t 2 >nul
goto :restart_loop