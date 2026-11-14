"""
Валидатор разворота — определяет, актуален ли разворот монеты для продолжения сделки.

Возвращает статус:
  green  — разворот актуален, продолжение безопасно
  orange — сомнительный разворот, есть признаки противодавления
  red    — разворот потерян, продолжение сделки крайне опасно

Результат содержит:
  status  — строка 'green' / 'orange' / 'red'
  reasons — список строк с пояснениями
  score   — число от 0.0 (полностью красный) до 1.0 (полностью зелёный)
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

log = logging.getLogger(__name__)


@dataclass
class ValidityResult:
    status: str          # 'green' | 'orange' | 'red'
    reasons: List[str]
    score: float         # 0.0 .. 1.0


def _classify(score: float, reasons: List[str]) -> ValidityResult:
    if score >= 0.55:
        status = 'green'
    elif score >= 0.30:
        status = 'orange'
    else:
        status = 'red'
    return ValidityResult(status=status, reasons=reasons, score=round(score, 3))


def evaluate_validity(
    symbol: str,
    side: str,                 # 'LONG' или 'SHORT'
    current_price: float,
    avg_entry_price: float,
    klines_1h: Optional[List],  # [(ts, o, h, l, c, v), ...] — последние 20-50 свечей
    klines_4h: Optional[List],  # те же
    ma_1h: Optional[float],
    ma_4h: Optional[float],
) -> ValidityResult:
    """
    Оценивает актуальность разворота для продолжения сделки.

    Для LONG:
      - green: цена выше средней, нижние тени на младших ТФ подтверждают покупку,
        нет новых минимумов ниже entry
      - orange: цена близка к средней, нет чёткого подтверждения
      - red: цена продолжает падать, новые минимумы ниже entry, MA направлена вниз

    Для SHORT — зеркально.
    """
    reasons: List[str] = []
    score = 0.5  # база — нейтрально

    if klines_1h is None or len(klines_1h) < 3:
        return _classify(score, ['Недостаточно данных 1h для валидации'])

    if klines_4h is None or len(klines_4h) < 3:
        reasons.append('Недостаточно данных 4h для валидации')
        score -= 0.15

    last = klines_1h[-1]
    prev = klines_1h[-2] if len(klines_1h) >= 2 else last
    open_p = float(last[1])
    close_p = float(last[4])
    high_p = float(last[2])
    low_p = float(last[3])
    vol = float(last[5])

    if side == 'LONG':
        score = _evaluate_long(
            current_price, avg_entry_price, open_p, close_p, high_p, low_p, vol,
            prev, klines_1h, klines_4h, ma_1h, ma_4h, reasons
        )
    elif side == 'SHORT':
        score = _evaluate_short(
            current_price, avg_entry_price, open_p, close_p, high_p, low_p, vol,
            prev, klines_1h, klines_4h, ma_1h, ma_4h, reasons
        )
    else:
        reasons.append(f'Неизвестное направление: {side}')

    return _classify(score, reasons)


def _evaluate_long(
    current_price: float,
    avg_entry_price: float,
    open_p: float,
    close_p: float,
    high_p: float,
    low_p: float,
    vol: float,
    prev,
    klines_1h: List,
    klines_4h: List,
    ma_1h: Optional[float],
    ma_4h: Optional[float],
    reasons: List[str],
) -> float:
    score = 0.5

    # 1. Цена относительно средней 1h
    if ma_1h and ma_1h > 0:
        dist_ma = (current_price - ma_1h) / ma_1h * 100
        if dist_ma > 0:
            score += 0.15
            reasons.append(f'Цена выше MA1h (+{dist_ma:.1f}%)')
        else:
            score -= 0.2
            reasons.append(f'Цена ниже MA1h ({dist_ma:.1f}%)')

    # 2. Цена относительно средней 4h
    if ma_4h and ma_4h > 0:
        dist_ma4 = (current_price - ma_4h) / ma_4h * 100
        if dist_ma4 > -1.0:
            score += 0.1
            reasons.append(f'Цена выше MA4h ({dist_ma4:.1f}%)')
        else:
            score -= 0.15
            reasons.append(f'Цена ниже MA4h ({dist_ma4:.1f}%)')

    # 3. Последние свечи 1h — формируют восходящий паттерн?
    bullish_candles = 0
    total_candles = min(5, len(klines_1h))
    for k in klines_1h[-total_candles:]:
        c = float(k[4])
        o = float(k[1])
        if c >= o:
            bullish_candles += 1
    if total_candles > 0:
        bull_ratio = bullish_candles / total_candles
        if bull_ratio >= 0.6:
            score += 0.15
            reasons.append(f'Бычьи свечи {bullish_candles}/{total_candles}')
        elif bull_ratio < 0.3:
            score -= 0.2
            reasons.append(f'Медвежьи свечи {total_candles - bullish_candles}/{total_candles}')

    # 4. Новые минимумы ниже entry?
    all_lows = [float(k[3]) for k in klines_1h[-10:]]
    if all_lows:
        new_low = min(all_lows)
        if new_low < avg_entry_price * 0.98:
            score -= 0.25
            reasons.append(f'Новый минимум ниже entry: {new_low:.6f}')
        else:
            score += 0.1
            reasons.append(f'Минимум выше entry: {new_low:.6f}')

    # 5. Объём последних свечей
    if vol > 0 and len(klines_1h) >= 10:
        avg_vol = sum(float(k[5]) for k in klines_1h[-10:]) / 10
        if avg_vol > 0:
            vol_ratio = vol / avg_vol
            if close_p > open_p and vol_ratio > 1.2:
                score += 0.1
                reasons.append(f'Объём выше среднего на растущей свече ({vol_ratio:.1f}x)')
            elif close_p < open_p and vol_ratio > 1.5:
                score -= 0.15
                reasons.append(f'Большой объём на падающей свече ({vol_ratio:.1f}x)')

    # 6. Тренд 4h — направление
    if len(klines_4h) >= 5:
        recent_4h = [float(k[4]) for k in klines_4h[-5:]]
        if recent_4h[-1] > recent_4h[0]:
            score += 0.1
            reasons.append('4h тренд восходящий')
        else:
            score -= 0.15
            reasons.append('4h тренд нисходящий')

    # 7. Текущая свеча — закрытие в верхней половине диапазона
    candle_range = high_p - low_p
    if candle_range > 0:
        close_pos = (close_p - low_p) / candle_range
        if close_pos > 0.6:
            score += 0.05
            reasons.append(f'Свеча закрылась в верхней части ({close_pos:.0%})')
        elif close_pos < 0.3:
            score -= 0.1
            reasons.append(f'Свеча закрылась в нижней части ({close_pos:.0%})')

    return min(1.0, max(0.0, score))


def _evaluate_short(
    current_price: float,
    avg_entry_price: float,
    open_p: float,
    close_p: float,
    high_p: float,
    low_p: float,
    vol: float,
    prev,
    klines_1h: List,
    klines_4h: List,
    ma_1h: Optional[float],
    ma_4h: Optional[float],
    reasons: List[str],
) -> float:
    score = 0.5

    # 1. Цена относительно средней 1h
    if ma_1h and ma_1h > 0:
        dist_ma = (ma_1h - current_price) / ma_1h * 100
        if dist_ma > 0:
            score += 0.15
            reasons.append(f'Цена ниже MA1h (-{dist_ma:.1f}%)')
        else:
            score -= 0.2
            reasons.append(f'Цена выше MA1h ({dist_ma:.1f}%)')

    # 2. Цена относительно средней 4h
    if ma_4h and ma_4h > 0:
        dist_ma4 = (ma_4h - current_price) / ma_4h * 100
        if dist_ma4 > -1.0:
            score += 0.1
            reasons.append(f'Цена ниже MA4h ({dist_ma4:.1f}%)')
        else:
            score -= 0.15
            reasons.append(f'Цена выше MA4h ({dist_ma4:.1f}%)')

    # 3. Последние свечи 1h — формируют нисходящий паттерн?
    bearish_candles = 0
    total_candles = min(5, len(klines_1h))
    for k in klines_1h[-total_candles:]:
        c = float(k[4])
        o = float(k[1])
        if c <= o:
            bearish_candles += 1
    if total_candles > 0:
        bear_ratio = bearish_candles / total_candles
        if bear_ratio >= 0.6:
            score += 0.15
            reasons.append(f'Медвежьи свечи {bearish_candles}/{total_candles}')
        elif bear_ratio < 0.3:
            score -= 0.2
            reasons.append(f'Бычьи свечи {total_candles - bearish_candles}/{total_candles}')

    # 4. Новые максимумы выше entry?
    all_highs = [float(k[2]) for k in klines_1h[-10:]]
    if all_highs:
        new_high = max(all_highs)
        if new_high > avg_entry_price * 1.02:
            score -= 0.25
            reasons.append(f'Новый максимум выше entry: {new_high:.6f}')
        else:
            score += 0.1
            reasons.append(f'Максимум ниже entry: {new_high:.6f}')

    # 5. Объём последних свечей
    if vol > 0 and len(klines_1h) >= 10:
        avg_vol = sum(float(k[5]) for k in klines_1h[-10:]) / 10
        if avg_vol > 0:
            vol_ratio = vol / avg_vol
            if close_p < open_p and vol_ratio > 1.2:
                score += 0.1
                reasons.append(f'Объём выше среднего на падающей свече ({vol_ratio:.1f}x)')
            elif close_p > open_p and vol_ratio > 1.5:
                score -= 0.15
                reasons.append(f'Большой объём на растущей свече ({vol_ratio:.1f}x)')

    # 6. Тренд 4h — направление
    if len(klines_4h) >= 5:
        recent_4h = [float(k[4]) for k in klines_4h[-5:]]
        if recent_4h[-1] < recent_4h[0]:
            score += 0.1
            reasons.append('4h тренд нисходящий')
        else:
            score -= 0.15
            reasons.append('4h тренд восходящий')

    # 7. Текущая свеча — закрытие в нижней половине диапазона
    candle_range = high_p - low_p
    if candle_range > 0:
        close_pos = (close_p - low_p) / candle_range
        if close_pos < 0.4:
            score += 0.05
            reasons.append(f'Свеча закрылась в нижней части ({close_pos:.0%})')
        elif close_pos > 0.7:
            score -= 0.1
            reasons.append(f'Свеча закрылась в верхней части ({close_pos:.0%})')

    return min(1.0, max(0.0, score))
