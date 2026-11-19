@echo off
cd /d "%~dp0"

REM --- Явно локальный режим (перебивает системные переменные) ---
set IS_RENDER=False

REM --- Виртуальное окружение ---
if not exist .venv (
    echo Creating venv...
    python -m venv .venv
    call .venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call .venv\Scripts\activate.bat
)

REM --- Проверка .env (не копируем молча!) ---
if not exist .env (
    echo.
    echo [WARN] Файл .env не найден!
    echo Создайте .env вручную по образцу .env.example и заполните:
    echo   IS_RENDER=False
    echo   DATABASE_URL=postgresql+asyncpg://...
    echo   BYBIT_API_KEY=...
    echo   BYBIT_API_SECRET=...
    echo.
    pause
    exit /b 1
)

REM --- Запуск (автоматический рестарт при аварии) ---
echo Starting Knife Bot locally (IS_RENDER=%IS_RENDER%)...
:restart_loop
.venv\Scripts\uvicorn.exe app.main:app --host 127.0.0.1 --port 8000
echo.
echo [!] Bot exited (crashed or stopped). Restarting in 2 seconds...
timeout /t 2 /nobreak >nul
goto :restart_loop