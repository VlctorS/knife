import tkinter as tk
from tkinter import ttk, messagebox
import requests
import threading
import time
from datetime import datetime, timezone

# ---------- Bybit API ----------
BYBIT_URL = "https://api.bybit.com/v5/market/kline"

PERIODS = {
    "15 дней": 15,
    "30 дней": 30,
    "60 дней": 60,
    "90 дней": 90,
}


def fetch_klines(symbol: str, days: int, interval: str = "60", log=print):
    """
    Скачиваем дневные/часовые свечи с Bybit (v5).
    interval: 1,3,5,15,30,60,120,240,360,720,D,W,M
    Bybit отдаёт максимум 1000 свечей за запрос — идём назад по времени.
    """
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 60 * 60 * 1000

    all_klines = []
    cursor_end = end_ms

    while True:
        params = {
            "category": "linear",   # USDT Perpetual
            "symbol": symbol.upper(),
            "interval": interval,
            "end": cursor_end,
            "limit": 1000,
        }
        r = requests.get(BYBIT_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {data.get('retMsg')}")

        batch = data["result"]["list"]
        if not batch:
            break

        # Bybit отдаёт по убыванию времени — приведём к возрастанию
        batch.sort(key=lambda x: int(x[0]))
        all_klines = batch + all_klines

        oldest_ts = int(batch[0][0])
        if oldest_ts <= start_ms or len(batch) < 1000:
            break

        cursor_end = oldest_ts - 1
        time.sleep(0.12)  # вежливость к API

    # фильтр по start
    all_klines = [k for k in all_klines if int(k[0]) >= start_ms]
    log(f"Получено свечей: {len(all_klines)}")
    return all_klines


def run_backtest(symbol: str, days: int, log=print):
    log(f"=== Бэктест {symbol.upper()} | период {days} дней | USDT Perpetual ===")
    log(f"Старт: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # Для периодов >30 дней берём 4h свечи (иначе слишком много данных), иначе 1h
    interval = "240" if days > 30 else "60"
    log(f"Интервал свечей: {interval} мин")

    klines = fetch_klines(symbol, days, interval, log)
    if not klines:
        raise RuntimeError("Нет данных по инструменту. Проверь тикер (например BTCUSDT).")

    # kline: [start, open, high, low, close, volume, turnover]
    highs = [float(k[2]) for k in klines]
    lows  = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    times = [int(k[0]) for k in klines]

    first_price = closes[0]
    last_price = closes[-1]

    min_price = min(lows)
    max_price = max(highs)
    min_idx = lows.index(min_price)
    max_idx = highs.index(max_price)
    min_time = datetime.fromtimestamp(times[min_idx] / 1000, tz=timezone.utc)
    max_time = datetime.fromtimestamp(times[max_idx] / 1000, tz=timezone.utc)

    # % изменения от цены открытия периода (берём close первой свечи)
    min_change = (min_price - first_price) / first_price * 100
    max_change = (max_price - first_price) / first_price * 100
    total_change = (last_price - first_price) / first_price * 100

    log("")
    log(f"Цена на начало периода : {first_price:,.4f} USDT")
    log(f"Цена на конец периода  : {last_price:,.4f} USDT")
    log("")
    log(f"Минимум за период  : {min_price:,.4f} USDT  ({min_change:+.2f}% от начала)")
    log(f"   время минимума  : {min_time.strftime('%Y-%m-%d %H:%M UTC')}")
    log("")
    log(f"Максимум за период : {max_price:,.4f} USDT  ({max_change:+.2f}% от начала)")
    log(f"   время максимума : {max_time.strftime('%Y-%m-%d %H:%M UTC')}")
    log("")
    log(f"Изменение цены за период (close→close): {total_change:+.2f}%")
    log(f"Диапазон max/min   : {(max_price/min_price - 1) * 100:+.2f}%")
    log("")
    log("=== Готово ===")

    return {
        "symbol": symbol.upper(),
        "days": days,
        "first": first_price,
        "last": last_price,
        "min": min_price,
        "min_time": min_time,
        "min_change": min_change,
        "max": max_price,
        "max_time": max_time,
        "max_change": max_change,
        "total_change": total_change,
        "candles": len(klines),
    }


# ---------- GUI ----------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Bybit Perpetual Backtest")
        self.attributes("-fullscreen", True)
        self.configure(bg="#121212")

        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self, bg="#1e1e1e", padx=12, pady=10)
        top.pack(side=tk.TOP, fill=tk.X)

        tk.Label(top, text="Монета (например BTCUSDT):",
                 fg="white", bg="#1e1e1e", font=("Consolas", 12)).pack(side=tk.LEFT)

        self.symbol_var = tk.StringVar(value="BTCUSDT")
        entry = tk.Entry(top, textvariable=self.symbol_var, width=20,
                         font=("Consolas", 12), bg="#2a2a2a", fg="white",
                         insertbackground="white")
        entry.pack(side=tk.LEFT, padx=8)

        tk.Label(top, text="Период:", fg="white", bg="#1e1e1e",
                 font=("Consolas", 12)).pack(side=tk.LEFT, padx=(15, 4))

        self.period_var = tk.StringVar(value=list(PERIODS.keys())[0])
        period_cb = ttk.Combobox(top, textvariable=self.period_var,
                                 values=list(PERIODS.keys()), state="readonly",
                                 width=10, font=("Consolas", 11))
        period_cb.pack(side=tk.LEFT)

        self.run_btn = tk.Button(top, text="▶ Запустить бэктест",
                                 command=self.start_backtest,
                                 bg="#00b894", fg="white",
                                 font=("Consolas", 12, "bold"),
                                 activebackground="#00a383", borderwidth=0,
                                 padx=12, pady=4)
        self.run_btn.pack(side=tk.LEFT, padx=15)

        self.copy_btn = tk.Button(top, text="📋 Копировать лог",
                                  command=self.copy_log,
                                  bg="#0984e3", fg="white",
                                  font=("Consolas", 12),
                                  activebackground="#0868b8", borderwidth=0,
                                  padx=12, pady=4)
        self.copy_btn.pack(side=tk.LEFT)

        self.status_lbl = tk.Label(top, text="Готов", fg="#b2bec3", bg="#1e1e1e",
                                   font=("Consolas", 11))
        self.status_lbl.pack(side=tk.LEFT, padx=15)

        # Hint про Escape
        tk.Label(top, text="(Esc — выйти)", fg="#636e72", bg="#1e1e1e",
                 font=("Consolas", 9)).pack(side=tk.RIGHT)

        # Логи
        bottom = tk.Frame(self, bg="#121212")
        bottom.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))

        self.log_text = tk.Text(bottom, bg="#0d0d0d", fg="#e6e6e6",
                                insertbackground="white",
                                font=("Consolas", 12), wrap="word",
                                relief=tk.FLAT, padx=10, pady=10)
        scroll = tk.Scrollbar(bottom, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.bind("<Escape>", lambda e: self.attributes("-fullscreen", False))

    def log(self, msg: str):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.update_idletasks()

    def copy_log(self):
        content = self.log_text.get("1.0", tk.END).strip()
        self.clipboard_clear()
        self.clipboard_append(content)
        self.status_lbl.config(text="✓ Лог скопирован в буфер")

    def start_backtest(self):
        symbol = self.symbol_var.get().strip().upper()
        if not symbol:
            messagebox.showerror("Ошибка", "Укажи тикер, например BTCUSDT")
            return
        days = PERIODS[self.period_var.get()]

        self.log_text.delete("1.0", tk.END)
        self.run_btn.config(state=tk.DISABLED, text="⏳ Загрузка...")
        self.status_lbl.config(text="Работаю...")

        def worker():
            try:
                run_backtest(symbol, days, log=self.log)
                self.status_lbl.config(text="✓ Готово")
            except Exception as e:
                self.log(f"❌ ОШИБКА: {e}")
                self.status_lbl.config(text="✗ Ошибка")
            finally:
                self.run_btn.config(state=tk.NORMAL, text="▶ Запустить бэктест")

        threading.Thread(target=worker, daemon=True).start()


if __name__ == "__main__":
    App().mainloop()