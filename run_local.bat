@echo off
cd /d C:\knife
if not exist .venv (
  python -m venv .venv
  call .venv\Scripts\activate.bat
  pip install -r requirements.txt
) else (
  call .venv\Scripts\activate.bat
)
if not exist .env copy .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload