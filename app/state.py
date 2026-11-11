import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from .config import settings


@dataclass
class SymbolState:
    symbol: str
    max_leverage: float = 0.0
    last_price: float = 0.0
    last_price_ts: float = 0.0
    ma: Optional[float] = None
    # история (ts, price) за последние window_minutes + запас
    price_history: Deque[tuple] = field(default_factory=deque)


@dataclass
class Alert:
    ts: float
    symbol: str
    kind: str          # "PUMP" | "DUMP"
    price_now: float
    ref_price: float   # цена N минут назад
    ma: Optional[float]
    change_pct: float  # (price_now / ref_price - 1) * 100
    timeframe: str = ""


class AppState:
    def __init__(self) -> None:
        self.symbols: Dict[str, SymbolState] = {}
        self.alerts: Deque[Alert] = deque(maxlen=settings.max_alerts)
        self.started_at: float = time.time()
        self.last_symbols_refresh: float = 0.0

    def upsert_symbol(self, symbol: str, max_leverage: float) -> None:
        s = self.symbols.get(symbol)
        if s is None:
            self.symbols[symbol] = SymbolState(symbol=symbol, max_leverage=max_leverage)
        else:
            s.max_leverage = max_leverage

    def set_symbols(self, data: List[dict]) -> None:
        """data = [{"symbol": ..., "maxLeverage": ...}]"""
        keep = {d["symbol"] for d in data}
        # удалить выпавшие
        for sym in list(self.symbols.keys()):
            if sym not in keep:
                del self.symbols[sym]
        for d in data:
            self.upsert_symbol(d["symbol"], float(d["maxLeverage"]))

    def on_tick(self, symbol: str, price: float, ts: float) -> None:
        s = self.symbols.get(symbol)
        if s is None:
            return
        s.last_price = price
        s.last_price_ts = ts
        s.price_history.append((ts, price))
        # режем историю: держим window*2 по времени
        horizon = ts - settings.window_minutes * 60 * 2
        while s.price_history and s.price_history[0][0] < horizon:
            s.price_history.popleft()

    def price_n_minutes_ago(self, symbol: str, minutes: int) -> Optional[float]:
        s = self.symbols.get(symbol)
        if s is None or not s.price_history:
            return None
        cutoff = s.last_price_ts - minutes * 60
        # ищем ближайшую точку к cutoff
        best = None
        best_diff = None
        for ts, price in s.price_history:
            d = abs(ts - cutoff)
            if best_diff is None or d < best_diff:
                best_diff = d
                best = price
            if ts > cutoff:
                break
        # требуем, чтобы точка была не новее cutoff+60сек и не старее cutoff-300сек
        if best is None:
            return None
        return best

    def add_alert(self, alert: Alert) -> None:
        self.alerts.appendleft(alert)


state = AppState()