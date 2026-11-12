import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from .config import settings


@dataclass
class CandleAnalysis:
    """Результат анализа свечи на «нож» (rejection candle)."""
    is_long_shadow: bool = False
    is_rejection: bool = False
    shadow_ratio: float = 0.0  # доля тени в теле свечи (high-low)/body
    upper_shadow: float = 0.0  # верхняя тень как % от диапазона
    lower_shadow: float = 0.0  # нижняя тень как % от диапазона
    body_ratio: float = 0.0    # тело как % от диапазона (high-low)
    is_pump: bool = False
    is_dump: bool = False
    trend_direction: str = ""  # "up" | "down" | "flat"
    trend_strength: float = 0.0  # 0..1


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


# =============================================================================
# Детекция «ножа» (rejection) и разделение всплеска с продолжением тренда
# =============================================================================

# Bybit kline: [start_ts, open, high, low, close, volume, turnover]
K_OPEN, K_HIGH, K_LOW, K_CLOSE, K_VOL = 1, 2, 3, 4, 5


def _to_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def ohlc(kline) -> Tuple[float, float, float, float]:
    """Из строки kline Bybit -> (open, high, low, close)."""
    return _to_float(kline[K_OPEN]), _to_float(kline[K_HIGH]), _to_float(kline[K_LOW]), _to_float(kline[K_CLOSE])


def analyze_candle(kline) -> CandleAnalysis:
    """
    Анализирует ОДНУ свечу на «нож» — резкий импульс, который вернулся.

    Ключевой признак ножа: цена ушла далеко от начала свечи (большой
    экстремум high/low), но закрылась близко к open — т.е. движение
    было отвергнуто рынком. Это НЕ продолжение тренда: у продолжения
    тело большое, а тени маленькие.

    Параметры порога берутся из settings, чтобы их можно было крутить.
    """
    res = CandleAnalysis()
    o, h, l, c = ohlc(kline)
    rng = h - l
    if rng <= 0 or o <= 0 or l <= 0:
        return res

    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l

    res.body_ratio = body / rng * 100.0
    res.upper_shadow = upper / rng * 100.0
    res.lower_shadow = lower / rng * 100.0
    res.shadow_ratio = res.upper_shadow + res.lower_shadow
    res.is_long_shadow = res.shadow_ratio >= settings.min_shadow_pct
    res.is_pump = c > o
    res.is_dump = c < o

    # Импульс относительно начала свечи: насколько_far дошёл экстремум
    move_pct = rng / o * 100.0
    if move_pct < settings.min_knife_move_pct:
        return res

    # Нож вниз (ищем LONG): длинная НИЖНЯЯ тень, закрытие у верха
    if res.lower_shadow >= settings.rejection_shadow_pct and body <= rng * settings.max_body_frac:
        res.is_rejection = True
        res.trend_direction = 'down'      # нож был направлен вниз
        res.trend_strength = res.lower_shadow / 100.0
    # Нож вверх (ищем SHORT): длинная ВЕРХНЯЯ тень, закрытие у низа
    elif res.upper_shadow >= settings.rejection_shadow_pct and body <= rng * settings.max_body_frac:
        res.is_rejection = True
        res.trend_direction = 'up'        # нож был направлен вверх
        res.trend_strength = res.upper_shadow / 100.0

    return res


def trend_context(klines, lookback: int = 6) -> Tuple[str, float]:
    """
    Направление и сила тренда по закрытиям последних свечей.

    Считаем наклон по первой и последней закрытой свече окна и
    согласованность (сколько свечей двигались в ту же сторону).
    Возвращает ('up'|'down'|'flat', strength 0..1).
    """
    closes = [_to_float(k[K_CLOSE]) for k in klines[-lookback:] if _to_float(k[K_CLOSE]) > 0]
    if len(closes) < 3:
        return 'flat', 0.0

    first, last = closes[0], closes[-1]
    net = (last / first - 1.0) * 100.0
    ups = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
    downs = len(closes) - 1 - ups
    steps = len(closes) - 1

    if net >= settings.trend_min_move_pct:
        direction = 'up'
        agreement = ups / steps if steps else 0.0
    elif net <= -settings.trend_min_move_pct:
        direction = 'down'
        agreement = downs / steps if steps else 0.0
    else:
        return 'flat', 0.0

    # Сила = компромисс между величиной движения и его однородностью
    strength = min(abs(net) / (settings.trend_min_move_pct * 4.0), 1.0) * 0.5 + agreement * 0.5
    return direction, strength


def classify_event(
    knife_tf,       # младший ТФ, где ищем сам нож (4h)
    anchor_tf,      # старший ТФ, контекст (1d)
    current_price: float,
    ma_knife: Optional[float],
    ma_anchor: Optional[float],
) -> dict:
    """
    Разделяем всплеск-нож и продолжение тренда.

    Вход против ножа имеет смысл только если старший ТФ НЕ тянет цену
    дальше по направлению ножа. Если на 1d стоит сильный тренд в ту же
    сторону, что и нож — это продолжение, и ловить его нельзя (летящий
    нож). Значит нужен либо флэт на старшем ТФ, либо нож против его тренда.

    knife_tf / anchor_tf — списки kline (в хронологическом порядке,
    последний элемент — ещё не закрытая свеча, поэтому берём [-2]).
    """
    out = {'is_spike': False, 'direction': None, 'confidence': 0.0, 'reason': '', 'analysis': None}

    if not knife_tf or len(knife_tf) < 3:
        out['reason'] = 'no knife timeframe data'
        return out

    an = analyze_candle(knife_tf[-2])
    out['analysis'] = an
    if not an.is_rejection:
        out['reason'] = (
            f'not a rejection candle (body={an.body_ratio:.0f}%, '
            f'lo={an.lower_shadow:.0f}%, hi={an.upper_shadow:.0f}%)'
        )
        return out

    anchor_dir, anchor_strength = ('flat', 0.0)
    if anchor_tf and len(anchor_tf) >= 4:
        anchor_dir, anchor_strength = trend_context(anchor_tf[:-1])

    # Нож вниз -> хотим LONG. Противник: нисходящий тренд на старшем ТФ.
    if an.trend_direction == 'down':
        if anchor_dir == 'down' and anchor_strength >= settings.max_anchor_trend:
            out['reason'] = f'trend continuation: {anchor_strength:.2f} downtrend on anchor TF'
            return out
        # Поддержка: цена должна быть уже не «на лету», а выше экстремума ножа
        knife_low = _to_float(knife_tf[-2][K_LOW])
        if current_price <= knife_low * 1.001:
            out['reason'] = 'price still at knife low (falling knife)'
            return out
        out['is_spike'] = True
        out['direction'] = 'long'
        out['confidence'] = _confidence(an, anchor_dir, anchor_strength)
        out['reason'] = f'knife down, anchor={anchor_dir}/{anchor_strength:.2f}'
        return out

    # Нож вверх -> хотим SHORT. Противник: восходящий тренд на старшем ТФ.
    if an.trend_direction == 'up':
        if anchor_dir == 'up' and anchor_strength >= settings.max_anchor_trend:
            out['reason'] = f'trend continuation: {anchor_strength:.2f} uptrend on anchor TF'
            return out
        knife_high = _to_float(knife_tf[-2][K_HIGH])
        if current_price >= knife_high * 0.999:
            out['reason'] = 'price still at knife high (rising blow-off)'
            return out
        out['is_spike'] = True
        out['direction'] = 'short'
        out['confidence'] = _confidence(an, anchor_dir, anchor_strength)
        out['reason'] = f'knife up, anchor={anchor_dir}/{anchor_strength:.2f}'
        return out

    out['reason'] = 'unknown rejection direction'
    return out


def _confidence(an: CandleAnalysis, anchor_dir: str, anchor_strength: float) -> float:
    """Чем длиннее тень и чем слабее встречный тренд — тем увереннее сигнал."""
    shadow = max(an.lower_shadow, an.upper_shadow)
    score = min((shadow - settings.rejection_shadow_pct) / 30.0, 1.0) * 0.5
    score += (1.0 - min(anchor_strength, 1.0)) * 0.3
    if anchor_dir == 'flat':
        score += 0.2
    else:
        score -= anchor_strength * 0.2
    return max(0.0, min(score, 1.0))


def moving_average(klines, period: int) -> Optional[float]:
    """MA по close последних `period` ЗАКРЫТЫХ свечей (последняя в списке — открытая)."""
    if not klines or len(klines) < period + 1:
        return None
    closes = [_to_float(k[K_CLOSE]) for k in klines[-(period + 1):-1]]
    closes = [c for c in closes if c > 0]
    if len(closes) < period:
        return None
    return sum(closes) / len(closes)


state = AppState()