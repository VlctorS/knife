# 📦 Локальный запуск бота Knife

## Требования
- Python 3.10+
- Виртуальное окружение (создаётся автоматически)

## Быстрый старт

### 1. Запуск через батник (рекомендуется)
```cmd
run_local.bat
```

Этот скрипт:
- Создаёт виртуальное окружение если нет `.venv`
- Автоматически создаёт `.env` с локальным SQLite
- Запускает бота на `http://127.0.0.1:8000`
- Автоматически перезапускает при crash

### 2. Ручной запуск

```cmd
REM Создание виртуального окружения
python -m venv .venv
call .venv\Scripts\activate

REM Установка зависимостей
pip install -r requirements.txt

REM Запуск
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## База данных

### По умолчанию: SQLite (локальная)
- Файл: `data/knife.db`
- Не требует внешней БД
- Идеально для тестирования и работы на ПК

### Переключение на PostgreSQL (Supabase/Render)
В файле `.env` измените:
```env
DATABASE_URL=postgresql+asyncpg://user:pass@host:port/dbname
```

## Конфигурация

Файл `.env` (создаётся автоматически):
```env
# Bybit API (опционально, для торговли)
BYBIT_API_KEY=your_api_key
BYBIT_API_SECRET=your_api_secret

# Локальная БД
DATABASE_URL=sqlite+aiosqlite:///data/knife.db

# Режим
IS_RENDER=False
```

## API эндпоинты

- `http://127.0.0.1:8000/` — веб-интерфейс
- `http://127.0.0.1:8000/api/status` — статус бота
- `http://127.0.0.1:8000/api/trades` — список сделок
- `http://127.0.0.1:8000/api/settings` — настройки

## Мониторинг логов

Бот логирует всё в консоль. Основные события:
- `[db] init_db завершена` — БД инициализирована
- `Symbols loaded: N` — загружены символы с Bybit
- `OPEN [SIDE] SYMBOL @ PRICE` — открыта сделка
- `SIGNAL [SIDE] SYMBOL` — найден сигнал для входа
- `VALIDITY CHANGE SYMBOL: green -> red` — изменён статус разворота

## Управление

- `Ctrl+C` — остановить бота
- Процесс автоматически перезапускается при crash

## Резервное копирование

Для SQLite просто копируйте файл:
```cmd
copy data\knife.db data\knife_backup_%date%.db
```
