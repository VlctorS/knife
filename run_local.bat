@echo off
cd /d C:\knife

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

REM --- Запуск (--reload удобен при разработке; для стабильности уберите его) ---
echo Starting Knife Bot locally (IS_RENDER=%IS_RENDER%)...
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
pause