"""Индикаторы: ATR и кластеризованные уровни поддержки/сопротивления.

Bybit kline формат: [start_ts, open, high, low, close, volume, turnover].
Последний элемент списка — ещё не закрытая свеча, поэтому расчёты
по «закрытым» данным используют срез [:-1].
"""

from typing import List, Optional


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def true_range(prev_close: float, high: float, low: float) -> float:
    """True Range одной свечи."""
    if prev_close <= 0:
        return max(high - low, 0.0)
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(klines, period: int = 14, use_closed_only: bool = True) -> Optional[float]:
    """ATR по Вилду (Wilder smoothing).

    Возвращает абсолютное значение ATR (в цене). None если данных мало.
    """
    if not klines or len(klines) < period + 2:
        return None
    src = list(klines[:-1]) if use_closed_only else list(klines)
    if len(src) < period + 1:
        return None

    trs: List[float] = []
    for i in range(1, len(src)):
        h, l, pc = _f(src[i][2]), _f(src[i][3]), _f(src[i - 1][4])
        if h <= 0 and l <= 0:
            continue
        trs.append(true_range(pc, h, l))
    if len(trs) < period:
        return None

    # Инициализация — простое среднее первых `period` TR, далее сглаживание.
    value = sum(trs[:period]) / period
    for tr in trs[period:]:
        value = (value * (period - 1) + tr) / period
    return value


def atr_pct(klines, period: int = 14) -> Optional[float]:
    """ATR в % от последней закрытой цены — удобно для порогов.»"""
    a = atr(klines, period)
    if a is None:
        return None
    last_close = _f(klines[-2][4]) if len(klines) >= 2 else _f(klines[-1][4])
    if last_close <= 0:
        return None
    return a / last_close * 100.0


def find_levels(klines, pivot_window: int = 3, min_touches: int = 2,
                cluster_tol_pct: float = 0.35) -> List[dict]:
    """Сырые pivot-уровни: локальные экстремумы, склеенные в кластеры.

    Возвращает [{'price', 'touches', 'kind' ('S'|'R'|'M'), 'last_ts', 'strength'}]
    в порядке возрастания цены. kind здесь — по природе пивота (high -> R,
    low -> S); относительно текущей цены тип переклассифицирует nearest_level.
    """
    candles = []
    for k in klines[:-1] if len(klines) > 1 else klines:
        o, h, l, c = _f(k[1]), _f(k[2]), _f(k[3]), _f(k[4])
        ts = _f(k[0])
        v = _f(k[5])
        if h <= 0 or l <= 0 or o <= 0:
            continue
        candles.append({'ts': ts, 'h': h, 'l': l, 'c': c, 'v': v})
    if len(candles) < pivot_window * 2 + 3:
        return []

    pivots: List[dict] = []
    w = pivot_window
    for i in range(w, len(candles) - w):
        cur = candles[i]
        left = candles[i - w:i]
        right = candles[i + 1:i + 1 + w]
        neigh = left + right
        if all(cur['h'] >= n['h'] for n in neigh):
            pivots.append({'price': cur['h'], 'kind': 'R', 'ts': cur['ts'], 'v': cur['v']})
        if all(cur['l'] <= n['l'] for n in neigh):
            pivots.append({'price': cur['l'], 'kind': 'S', 'ts': cur['ts'], 'v': cur['v']})
    if not pivots:
        return []

    pivots.sort(key=lambda p: p['price'])
    tol = cluster_tol_pct / 100.0
    clusters: List[dict] = []
    for p in pivots:
        last = clusters[-1] if clusters else None
        if last and abs(p['price'] - last['price']) / last['price'] <= tol:
            # Средневзвешенная по объёму цена кластера
            total_v = last['sum_v'] + p['v']
            last['price'] = (last['price'] * last['sum_v'] + p['price'] * p['v']) / total_v if total_v > 0 else (last['price'] + p['price']) / 2
            last['sum_v'] = total_v
            last['touches'] += 1
            last['last_ts'] = max(last['last_ts'], p['ts'])
            last['kinds'][p['kind']] = last['kinds'].get(p['kind'], 0) + 1
        else:
            clusters.append({
                'price': p['price'],
                'sum_v': p['v'],
                'touches': 1,
                'last_ts': p['ts'],
                'kinds': {p['kind']: 1},
            })

    avg_vol = (sum(c['sum_v'] for c in clusters) / max(sum(c['touches'] for c in clusters), 1)) or 0
    levels = []
    span = max(len(candles), 1)
    for cl in clusters:
        if cl['touches'] < min_touches:
            continue
        kind = max(cl['kinds'], key=cl['kinds'].get)
        freshness = cl['last_ts'] / max(candles[-1]['ts'], 1) if candles[-1]['ts'] > 0 else 0.5
        strength = round(min(cl['touches'] / 5.0, 1.0) * 0.6 + freshness * 0.4, 3)
        levels.append({
            'price': round(cl['price'], 8),
            'touches': cl['touches'],
            'kind': kind,
            'last_ts': cl['last_ts'],
            'strength': strength,
        })
    levels.sort(key=lambda x: x['price'])
    return levels


def nearest_level(levels: List[dict], price: float, direction: str,
                  within_pct: float) -> Optional[dict]:
    """Ближайший уровень в указанном направлении от цены.

    direction='below' — самый высокий уровень строго ниже price;
    direction='above' — самый низкий уровень строго выше price.
    within_pct — максимальное расстояние поиска в % (None = без лимита).
    """
    if not levels or price <= 0:
        return None
    cand = None
    for lv in levels:
        p = lv['price']
        if direction == 'below' and p < price:
            dist = (price - p) / price * 100
            if within_pct is not None and dist > within_pct:
                continue
            if cand is None or p > cand['price']:
                cand = {**lv, 'distance_pct': round(-dist, 2)}
        elif direction == 'above' and p > price:
            dist = (p - price) / price * 100
            if within_pct is not None and dist > within_pct:
                continue
            if cand is None or p < cand['price']:
                cand = {**lv, 'distance_pct': round(dist, 2)}
    return cand
