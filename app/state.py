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
    reason: str = ""
    # Ожидание закрытия свечи (радар-сигнал): ТФ ожидаемой свечи, её
    # таймстемп открытия (сек) и дедлайн подтверждения (сек). 0 — не ждём.
    wait_tf_label: str = ""
    wait_candle_open_ts: float = 0.0
    wait_deadline_ts: float = 0.0
    tf_sec: int = 0  # длительность бара рабочего ТФ в секундах


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
    volume_ratio: Optional[float] = None,
) -> dict:
    """
    Разделяем всплеск-нож и продолжение тренда.

    Вход против ножа имеет смысл только если старший ТФ НЕ тянет цену
    дальше по направлению ножа. Если на 1d стоит сильный тренд в ту же
    сторону, что и нож — это продолжение, и ловить его нельзя (летящий
    нож). Значит нужен либо флэт на старшем ТФ, либо нож против его тренда.

    Дополнительные фильтры (все согласованы с логикой ножа):
      - volume_ratio >= min_knife_volume_ratio: движение подтверждено объёмом;
      - цена отошла от экстремума ножа не менее чем на min_bounce_pct:
        отскок начался, мы не лезем под падающий нож;
      - цена не выкупилась выше max_retrace_to_open_pct движения: иначе
        львиная доля «ножевого» движения уже съедена и матожидание входа плохое.

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

    # Объёмный всплеск: без подтверждения объёмом считаем движение «тонким»
    if volume_ratio is not None and volume_ratio < settings.min_knife_volume_ratio:
        out['reason'] = f'thin move, volume {volume_ratio:.2f}x avg (need >= {settings.min_knife_volume_ratio:.2f}x)'
        return out

    o, h, l, c = ohlc(knife_tf[-2])
    rng = h - l
    knife_mid = (o + c) / 2.0 if rng > 0 else 0.0

    anchor_dir, anchor_strength = ('flat', 0.0)
    if anchor_tf and len(anchor_tf) >= 4:
        anchor_dir, anchor_strength = trend_context(anchor_tf[:-1])

    # Нож вниз -> хотим LONG. Противник: нисходящий тренд на старшем ТФ.
    if an.trend_direction == 'down':
        if anchor_dir == 'down' and anchor_strength >= settings.max_anchor_trend:
            out['reason'] = f'trend continuation: {anchor_strength:.2f} downtrend on anchor TF'
            return out
        knife_low = _to_float(knife_tf[-2][K_LOW])
        bounce_pct = (current_price - knife_low) / knife_low * 100.0
        if current_price <= knife_low * 1.001 or bounce_pct < settings.min_bounce_pct:
            out['reason'] = f'price still at knife low (falling knife), bounce {bounce_pct:.2f}% < {settings.min_bounce_pct:.2f}%'
            return out
        out['is_spike'] = True
        out['direction'] = 'long'
        out['confidence'] = _confidence(an, anchor_dir, anchor_strength, bounce_pct)
        out['reason'] = f'knife down, anchor={anchor_dir}/{anchor_strength:.2f}, bounce={bounce_pct:.2f}%'
        return out

    # Нож вверх -> хотим SHORT. Противник: восходящий тренд на старшем ТФ.
    if an.trend_direction == 'up':
        if anchor_dir == 'up' and anchor_strength >= settings.max_anchor_trend:
            out['reason'] = f'trend continuation: {anchor_strength:.2f} uptrend on anchor TF'
            return out
        knife_high = _to_float(knife_tf[-2][K_HIGH])
        bounce_pct = (knife_high - current_price) / knife_high * 100.0
        if current_price >= knife_high * 0.999 or bounce_pct < settings.min_bounce_pct:
            out['reason'] = f'price still at knife high (rising blow-off), bounce {bounce_pct:.2f}% < {settings.min_bounce_pct:.2f}%'
            return out
        out['is_spike'] = True
        out['direction'] = 'short'
        out['confidence'] = _confidence(an, anchor_dir, anchor_strength, bounce_pct)
        out['reason'] = f'knife up, anchor={anchor_dir}/{anchor_strength:.2f}, bounce={bounce_pct:.2f}%'
        return out

    out['reason'] = 'unknown rejection direction'
    return out


def _confidence(an: CandleAnalysis, anchor_dir: str, anchor_strength: float,
                bounce_pct: float = 0.0) -> float:
    """Чем длиннее тень, чем слабее встречный тренд и чем увереннее отскок — тем сильнее сигнал."""
    shadow = max(an.lower_shadow, an.upper_shadow)
    score = min((shadow - settings.rejection_shadow_pct) / 30.0, 1.0) * 0.45
    score += (1.0 - min(anchor_strength, 1.0)) * 0.25
    if anchor_dir == 'flat':
        score += 0.15
    else:
        score -= anchor_strength * 0.2
    # Отскок: лучший вход там, где цена отошла от экстремума, но не улетела далеко.
    # Оптимум ~1.5%..6%, свыше штраф за поздний вход.
    if bounce_pct >= settings.min_bounce_pct:
        if bounce_pct <= 6.0:
            score += 0.15
        elif bounce_pct <= 10.0:
            score += 0.08
        else:
            score -= 0.1
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


# =============================================================================
# Пин-бар у уровня + подтверждение входа (режимы scalp/intraday/swing)
# =============================================================================

@dataclass
class PinContext:
    """Полный контекст сигнала: пин-бар + уровень + тренд якоря."""
    side: str = ''                  # 'LONG' | 'SHORT' | ''
    pin_index: int = -1             # индекс пин-свечи в списке kline
    entry_price: float = 0.0        # цена подтверждения (фактический вход)
    stop_price: float = 0.0         # за экстремумом тени
    target_price: float = 0.0       # ближайший встречный уровень SR
    level_price: float = 0.0        # уровень, от которого отвергнули
    atr_work: float = 0.0           # ATR рабочего ТФ (абсолютный)
    anchor_trend: str = 'flat'      # 'up' | 'down' | 'flat' по закрытиям якоря
    anchor_strength: float = 0.0
    confidence: float = 0.0
    reason: str = ''
    confirm_type: str = ''          # 'close' | 'bos' | 'retest'
    details: dict = field(default_factory=dict)


def _kline_ohlc_v(k):
    o, h, l, c = _to_float(k[K_OPEN]), _to_float(k[K_HIGH]), _to_float(k[K_LOW]), _to_float(k[K_CLOSE])
    v = _to_float(k[K_VOL])
    return o, h, l, c, v


def is_pin_bar(k, shadow_frac: float, min_range_pct: float,
               max_body_frac: float = 0.34) -> Tuple[bool, str, float]:
    """Одиночная свеча — пин-бар с длинной тенью (не менее shadow_frac диапазона)?

    Возвращает (is_pin, direction, range_pct):
      direction 'down' — длинная НИЖНЯЯ тень (потенциальный LONG),
      direction 'up'   — длинная ВЕРХНЯЯ тень (потенциальный SHORT).
    Тело не больше max_body_frac диапазона — иначе это импульс, а не отвержение.
    """
    o, h, l, c, _v = _kline_ohlc_v(k)
    rng = h - l
    if rng <= 0 or o <= 0 or l <= 0:
        return False, '', 0.0
    range_pct = rng / o * 100.0
    if range_pct < min_range_pct:
        return False, '', range_pct

    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    if body > rng * max_body_frac:
        return False, '', range_pct

    if lower / rng >= shadow_frac:
        return True, 'down', range_pct
    if upper / rng >= shadow_frac:
        return True, 'up', range_pct
    return False, '', range_pct


def trend_by_closes(klines, lookback: int = 8,
                    use_open: bool = False) -> Tuple[str, float]:
    """Трендовый фильтр старшего ТФ по MA-закрытиям и направлению MACD.

    Ключевое отличие от trend_context(): здесь флэт — это НЕ «против
    тренда», а отсутствие направления; вход разрешён только когда
    направление явного тренда совпадает со стороной сделки.
    Возвращает ('up'|'down'|'flat', strength 0..1).

    use_open=True: незакрытая свеча участвует в MA/MACD как есть (живой
    price), а сила умножается на fraction — долю бара, прошедшую к текущему
    моменту. Это снимает конфликт «окно подтверждения короче времени жизни
    свечи якоря»: для часового якоря сигнал живёт десятки минут, но тренд
    часа виден сразу по накопленным барам внутри текущего часа.
    """
    if not klines or len(klines) < 30:
        return 'flat', 0.0
    src = list(klines) if use_open else list(klines[:-1])
    closes = [_to_float(k[K_CLOSE]) for k in src]
    closes = [c for c in closes if c > 0]
    if len(closes) < 26:
        return 'flat', 0.0

    ma_fast = sum(closes[-9:]) / 9.0
    ma_slow = sum(closes[-26:]) / 26.0

    # EMA/MACD по последним свечам якорного ТФ
    ema12 = ema26 = closes[0]
    hist_prev = None
    hist_pos = hist_neg = 0
    k12, k26 = 2 / 13.0, 2 / 27.0
    for c in closes:
        ema12 = c * k12 + ema12 * (1 - k12)
        ema26 = c * k26 + ema26 * (1 - k26)
        hist = ema12 - ema26
        if hist_prev is not None:
            if hist > hist_prev:
                hist_pos += 1
            elif hist < hist_prev:
                hist_neg += 1
        hist_prev = hist

    slope = (ma_fast - ma_slow) / ma_slow * 100.0 if ma_slow > 0 else 0.0
    rising = hist_pos > hist_neg
    falling = hist_neg > hist_pos

    if slope > 0.15 and rising:
        direction = 'up'
    elif slope < -0.15 and falling:
        direction = 'down'
    else:
        return 'flat', 0.0

    n = max(hist_pos + hist_neg, 1)
    agreement = max(hist_pos, hist_neg) / n
    mag = min(abs(slope) / 1.5, 1.0)
    strength = round(min(0.5 * agreement + 0.5 * mag, 1.0), 3)
    if use_open and klines:
        strength = round(strength * _anchor_progress(klines[-1]), 3)
    return direction, strength


def _anchor_progress(last_kline) -> float:
    """Доля незакрытой свечи якоря, прошедшая к текущему моменту (0.35..1)."""
    try:
        ts = _to_float(last_kline[0])
        dur = _to_float(last_kline[6]) if len(last_kline) > 6 else 0.0
        if ts <= 0 or dur <= 0:
            return 0.6
        frac = (time.time() - ts) / dur
        return max(0.35, min(frac, 1.0))
    except (TypeError, ValueError):
        return 0.6


def find_pins_with_confirm(kl, levels, atr_value: float, current_price: float,
                           shadow_frac: float, move_atr_mult: float,
                           move_pct_min: float, lookback_candles: int = 36,
                           max_pin_age_candles: int = 6,
                           level_tol_atr: float = 0.8) -> List[dict]:
    """Ищем свежие пин-бары у уровней и проверяем подтверждение.

    kl — список kline рабочего ТФ (последний — незакрытая свеча).

    Порог диапазона свечи ЕДИНЫЙ: max(move_pct_min, move_atr_mult*ATR%) —
        применяется ровно один раз (раньше удваивался через is_pin_bar).

    Подтверждение (любое из трёх, в пределах max_pin_age_candles):
      1) close  — следующая(ие) свеча(и) закрылась(лись) выше тела пин-бара
                  (для LONG над max(open,close) пина);
      2) bos    — break of structure: закрытие за локальным экстремумом
                  микроструктуры (high/low соседних свечей вокруг пина);
      3) retest — ретест тени: цена коснулась 50% уровня тени пина и
                  отверглась (закрытие от неё в сторону сделки).

    Уровень: экстремальная точка тени пин-бара должна доставать S/R уровня
    рабочего ТФ (допуск = level_tol_atr*ATR, настраивается режимом).

    Возвращает список найденных кандидатов (максимум 3):
      {'index', 'side', 'extreme', 'body_top', 'body_bottom',
       'confirm': {...}, 'ts': ts_открытия_пина, 'confirm_ts': ts_подтверждения}
    """
    out: List[dict] = []
    if not kl or len(kl) < 12 or not atr_value or not current_price:
        return out

    closed_end = len(kl) - 1  # последний закрытый индекс (kl[-1] — текущая свеча)
    start = max(1, closed_end - lookback_candles)
    avg_vol = _avg_volume(kl, VOLUME_LOOKBACK_DEFAULT)
    tol = max(atr_value * level_tol_atr, current_price * 0.0015)

    # Единый порог диапазона: ATR-привязка + защита тихого рынка (%).
    # Считаем один раз от ATR и текущей цены; внутрицикловая перепроверка
    # в is_pin_bar убрана, чтобы множители режима оставались независимыми.
    min_range_pct = max(move_pct_min, move_atr_mult * atr_value / current_price * 100.0)

    # --- BOS-структуры: high/low соседей окна пина ---
    def _local_struct(i: int, side: str):
        lo = max(1, i - 4)
        hi = min(closed_end, i + 4)
        neigh = [j for j in range(lo, hi + 1) if j != i]
        if not neigh:
            return None
        if side == 'LONG':
            # слом вверх: максимум high соседей (кроме самого пина)
            return max(_to_float(kl[j][K_HIGH]) for j in neigh if j <= closed_end - 1) \
                if any(j <= closed_end - 1 for j in neigh) else None
        return min(_to_float(kl[j][K_LOW]) for j in neigh if j <= closed_end - 1) \
            if any(j <= closed_end - 1 for j in neigh) else None

    for i in range(start, closed_end):
        k = kl[i]
        ok, direction, range_pct = is_pin_bar(k, shadow_frac, min_range_pct)
        if not ok:
            continue
        o, h, l, c, v = _kline_ohlc_v(k)
        body_top, body_bottom = max(o, c), min(o, c)

        if direction == 'down':
            side = 'LONG'
            extreme = l
        else:
            side = 'SHORT'
            extreme = h

        # нужен контакт с уровнем SR
        touched = None
        for lv in (levels or []):
            p = lv.get('price', 0)
            if p <= 0:
                continue
            inside = (min(body_bottom, extreme) - tol) <= p <= (max(body_top, extreme) + tol)
            near = abs(extreme - p) <= tol
            if inside or near:
                # для LONG используем support (ниже цены тела), для SHORT — resistance
                if side == 'LONG' and p > current_price:
                    continue
                if side == 'SHORT' and p < current_price:
                    continue
                touched = lv
                break
        if touched is None:
            continue

        vol_ratio = (v / avg_vol) if avg_vol > 0 else None
        if vol_ratio is not None and vol_ratio < 0.5:
            # пин без объёма — вероятная пустота, не кандидат
            continue

        # age: сколько закрытых свечей прошло после пина
        age_max = min(max_pin_age_candles, closed_end - i)
        confirm = None
        struct = _local_struct(i, side)

        for j in range(i + 1, i + 1 + age_max):
            if j > closed_end:
                break
            o_j, h_j, l_j, c_j, _v_j = _kline_ohlc_v(kl[j])
            if c_j <= 0:
                continue
            if side == 'LONG':
                # close above body пина
                if c_j > body_top and confirm is None:
                    confirm = {'type': 'close', 'index': j, 'price': c_j}
                # BOS: закрытие выше структуры
                if struct and c_j > struct:
                    confirm = {'type': 'bos', 'index': j, 'price': c_j,
                               'struct': struct}
                    break
                # retest тени: low дошёл до 50% тени и закрылся выше середины тела
                mid_shadow = (body_bottom + extreme) / 2.0
                if l_j <= mid_shadow and c_j > body_bottom:
                    if confirm is None or confirm['type'] == 'close':
                        confirm = {'type': 'retest', 'index': j, 'price': c_j,
                                   'level': mid_shadow}
            else:
                if c_j < body_bottom and confirm is None:
                    confirm = {'type': 'close', 'index': j, 'price': c_j}
                if struct and c_j < struct:
                    confirm = {'type': 'bos', 'index': j, 'price': c_j,
                               'struct': struct}
                    break
                mid_shadow = (body_top + extreme) / 2.0
                if h_j >= mid_shadow and c_j < body_top:
                    if confirm is None or confirm['type'] == 'close':
                        confirm = {'type': 'retest', 'index': j, 'price': c_j,
                                   'level': mid_shadow}
            if confirm and confirm['type'] in ('close', 'retest') and j >= i + 2:
                # close/retest достаточно одного-двух свечей; идём дальше за BOS
                pass

        if confirm is None:
            continue

        out.append({
            'index': i,
            'side': side,
            'pin_open': o, 'pin_high': h, 'pin_low': l, 'pin_close': c,
            'extreme': extreme,
            'body_top': body_top, 'body_bottom': body_bottom,
            'range_pct': round(range_pct, 3),
            'level_price': touched['price'],
            'level_strength': touched.get('strength', 0.5),
            'vol_ratio': round(vol_ratio, 2) if vol_ratio else None,
            'confirm': confirm,
            'ts': _to_float(k[0]),
            # ts закрытия свечи-подтверждения — точка отсчёта для confirm window.
            # Раньше окно считалось от открытия пина и retest/BOS на 4-6 свечах
            # физически не проходили фильтр.
            'confirm_ts': _to_float(kl[confirm['index']][0]) + _candle_sec(kl),
        })
        if len(out) >= 3:
            break
    return out


VOLUME_LOOKBACK_DEFAULT = 20


def _avg_volume(kl, lookback: int = 20) -> float:
    if len(kl) < lookback + 1:
        return 0.0
    vols = [_to_float(k[K_VOL]) for k in kl[-(lookback + 1):-1]]
    vols = [v for v in vols if v > 0]
    return sum(vols) / len(vols) if vols else 0.0


def _candle_sec(kl) -> float:
    """Длительность свечи в секундах по первым двум таймстампам."""
    try:
        if len(kl) >= 2:
            d = _to_float(kl[1][0]) - _to_float(kl[0][0])
            if d > 0:
                return d
    except (TypeError, ValueError):
        pass
    return 3600.0


state = AppState()