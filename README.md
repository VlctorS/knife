# knife — Bybit Pump/Dump Monitor

Мониторит USDT-перпетуалы Bybit, ловит памп/дамп ≥ THRESHOLD (в %)
относительно цены N минут назад. Дополнительно показывает MA(1h, MA_PERIOD).

## Локальный запуск (Windows)
```bat
cd C:\knife
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload