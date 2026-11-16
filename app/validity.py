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
    # Подробное обоснование смены цвета (для истории и UI)
    metrics: Optional[dict] = None
    verdict: Optional[dict] = None


def _classify(score: float, reasons: List[str]) -> ValidityResult:
    """Через score проходят только мягкие факторы.

    Красный цвет не может быть выставлен «просто так» — он ставится
    исключительно structural_exit в evaluate_validity, когда разворот
    реально сломан (пробит entry + объём + нет отскока).
    """
    if score >= 0.45:
        status = 'green'
    elif score >= 0.25:
        status = 'orange'
    else:
        status = 'orange'  # мягкие факторы до красного не доводят
    return ValidityResult(status=status, reasons=reasons, score=round(score, 3))


# Пороги структурного слома разворота (красный цвет)
NEW_EXTREME_PCT = 2.0        # новый экстремум глубже entry не менее чем на 2%
STRUCT_MIN_HOURS = 4         # минимум часов в сделке до возможного красного
STRUCT_CONFIRM_CANDLES = 2   # закрытых 1h-свечи без возврата к средней
STRUCT_VOLUME_RATIO = 1.3    # объём свечи-пробойника к среднему за 20 свечей


def evaluate_validity(
    symbol: str,
    side: str,                 # 'LONG' или 'SHORT'
    current_price: float,
    avg_entry_price: float,
    klines_1h: Optional[List],  # [(ts, o, h, l, c, v), ...] — последние 20-50 свечей
    klines_4h: Optional[List],  # те же
    ma_1h: Optional[float],
    ma_4h: Optional[float],
    age_hours: Optional[float] = None,   # сколько часов открыта сделка
) -> ValidityResult:
    """
    Оценивает актуальность разворота для продолжения сделки.

    Для LONG:
      - green: цена выше средней, нижние тени на младших ТФ подтверждают покупку,
        нет новых минимумов ниже entry
      - orange: цена близка к средней, есть признаки противодавления
      - red: СТРУКТУРНЫЙ слом разворота — новый экстремум глубже entry на 2%+,
        подтверждённый объёмом, без отскока несколько закрытых свечей,
        и сделка живет достаточно долго. Только красный является
        триггером для закрытия позиции.

    Для SHORT — зеркально.

    Красный НЕ выставляется за «цену просто дёрнулась вниз», за «были
    медвежьи свечи» и т.п. мягкие факторы — они снижают score и могут
    дать только orange. Также красный не ставится в cooldown-период
    сразу после входа (первые STRUCT_MIN_HOURS часов).
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

    metrics: dict = {}

    if side == 'LONG':
        score = _evaluate_long(
            current_price, avg_entry_price, open_p, close_p, high_p, low_p, vol,
            prev, klines_1h, klines_4h, ma_1h, ma_4h, reasons, metrics
        )
    elif side == 'SHORT':
        score = _evaluate_short(
            current_price, avg_entry_price, open_p, close_p, high_p, low_p, vol,
            prev, klines_1h, klines_4h, ma_1h, ma_4h, reasons, metrics
        )
    else:
        reasons.append(f'Неизвестное направление: {side}')

    result = _classify(score, reasons)
    result.metrics = metrics

    # --- Структурные критерии красного (единственный путь к red) ---
    checks, blockers = _structural_checks(side, current_price, avg_entry_price,
                                          klines_1h, metrics)
    in_cooldown = age_hours is not None and age_hours < STRUCT_MIN_HOURS

    verdict_reasons: List[str] = []
    red_candidate = all(c['passed'] for c in checks)
    for c in checks:
        mark = 'выполнено' if c['passed'] else 'НЕ выполнено'
        verdict_reasons.append(f"[{c['name']}] {mark}: {c['detail']}")
    if in_cooldown:
        verdict_reasons.insert(
            0,
            f"[cooldown] красен невозможна первые {STRUCT_MIN_HOURS} ч после входа "
            f"(сделке {age_hours:.1f} ч)"
        )

    red_candidate = all(c['passed'] for c in checks)
    if red_candidate and in_cooldown:
        reasons_note = ('структурные признаки слома есть, но красный запрещён '
                        f'в cooldown первые {STRUCT_MIN_HOURS} ч после входа')
    elif red_candidate:
        reasons_note = 'разворот структурно сломан: все структурные условия подтверждены'
    else:
        reasons_note = ('структурный слом не подтверждён: '
                        + ('; '.join(blockers) if blockers else 'условия не выполнены'))

    if red_candidate and not in_cooldown:
        result.status = 'red'
        result.reasons = reasons + [c['detail'] for c in checks]
        metrics['structural_red'] = True
    else:
        metrics['structural_red'] = False

    result.verdict = {
        'decision': result.status,
        'note': reasons_note,
        'checks': checks,
        'in_cooldown': in_cooldown,
        'age_hours': round(age_hours, 2) if age_hours is not None else None,
        'explanation': '; '.join(verdict_reasons),
    }
    return result


def _structural_checks(side: str, current_price: float, avg_entry: float,
                       klines_1h: List, metrics: dict):
    """Возвращает (checks, blockers) — список структурных условий слома разворота."""
    checks: List[dict] = []
    blockers: List[str] = []

    closed = klines_1h[:-1]  # последняя свеча ещё не закрыта
    recent = closed[-STRUCT_CONFIRM_CANDLES:] if len(closed) >= STRUCT_CONFIRM_CANDLES else closed

    if side == 'LONG':
        # 1. Новый минимум глубже entry минимум на NEW_EXTREME_PCT
        lows = [float(k[3]) for k in closed[-10:]] if closed else []
        min_low = min(lows) if lows else current_price
        depth_pct = (avg_entry - min_low) / avg_entry * 100 if avg_entry > 0 else 0.0
        passed_1 = depth_pct >= NEW_EXTREME_PCT
        checks.append({
            'name': 'новый_минимум',
            'passed': passed_1,
            'detail': f'мин. low за 10 закрытых свечей = {min_low:.6f}, '
                      f'просадка ниже средней на {depth_pct:.2f}% (нужно ≥ {NEW_EXTREME_PCT}%)',
        })
        if not passed_1:
            blockers.append('нет нового глубокого минимума ниже средней')

        # 2. Закрытия последних свечей ниже entry (отскока нет)
        closes_below = [k for k in recent if float(k[4]) < avg_entry]
        passed_2 = len(recent) >= STRUCT_CONFIRM_CANDLES and len(closes_below) == len(recent)
        checks.append({
            'name': 'без_отскока',
            'passed': passed_2,
            'detail': f'{len(closes_below)} из {len(recent)} последних закрытых 1h-свечей '
                      f'закрылись ниже средней (нужно все {STRUCT_CONFIRM_CANDLES})',
        })
        if not passed_2:
            blockers.append('есть отскок — свечи закрываются выше средней')

        # 3. Объём пробоя выше среднего
        vols = [float(k[5]) for k in closed[-20:]] if closed else []
        avg_vol = sum(vols) / len(vols) if vols else 0.0
        breach_candle = min(recent, key=lambda k: float(k[3])) if recent else None
        breach_vol = float(breach_candle[5]) if breach_candle else 0.0
        vol_ratio = breach_vol / avg_vol if avg_vol > 0 else 0.0
        passed_3 = avg_vol > 0 and vol_ratio >= STRUCT_VOLUME_RATIO
        checks.append({
            'name': 'объём_пробоя',
            'passed': passed_3,
            'detail': f'объём свечи-пробойника {breach_vol:.0f} vs средний {avg_vol:.0f} '
                      f'= {vol_ratio:.2f}x (нужно ≥ {STRUCT_VOLUME_RATIO}x)',
        })
        if not passed_3:
            blockers.append('пробой не подтверждён объёмом')

        # 4. Текущая цена не вернулась к средней
        passed_4 = current_price < avg_entry * 0.99
        checks.append({
            'name': 'цена_под_средней',
            'passed': passed_4,
            'detail': f'текущая цена {current_price:.6f} '
                      f'{"ниже" if passed_4 else "не ниже"} средней {avg_entry:.6f} более чем на 1%',
        })
        if not passed_4:
            blockers.append('цена уже вернулась к средней')
    else:  # SHORT — зеркально
        highs = [float(k[2]) for k in closed[-10:]] if closed else []
        max_high = max(highs) if highs else current_price
        depth_pct = (max_high - avg_entry) / avg_entry * 100 if avg_entry > 0 else 0.0
        passed_1 = depth_pct >= NEW_EXTREME_PCT
        checks.append({
            'name': 'новый_максимум',
            'passed': passed_1,
            'detail': f'макс. high за 10 закрытых свечей = {max_high:.6f}, '
                      f'выход выше средней на {depth_pct:.2f}% (нужно ≥ {NEW_EXTREME_PCT}%)',
        })
        if not passed_1:
            blockers.append('нет нового высокого максимума выше средней')

        closes_above = [k for k in recent if float(k[4]) > avg_entry]
        passed_2 = len(recent) >= STRUCT_CONFIRM_CANDLES and len(closes_above) == len(recent)
        checks.append({
            'name': 'без_отскока',
            'passed': passed_2,
            'detail': f'{len(closes_above)} из {len(recent)} последних закрытых 1h-свечей '
                      f'закрылись выше средней (нужно все {STRUCT_CONFIRM_CANDLES})',
        })
        if not passed_2:
            blockers.append('есть откат — свечи закрываются ниже средней')

        vols = [float(k[5]) for k in closed[-20:]] if closed else []
        avg_vol = sum(vols) / len(vols) if vols else 0.0
        breach_candle = max(recent, key=lambda k: float(k[2])) if recent else None
        breach_vol = float(breach_candle[5]) if breach_candle else 0.0
        vol_ratio = breach_vol / avg_vol if avg_vol > 0 else 0.0
        passed_3 = avg_vol > 0 and vol_ratio >= STRUCT_VOLUME_RATIO
        checks.append({
            'name': 'объём_пробоя',
            'passed': passed_3,
            'detail': f'объём свечи-пробойника {breach_vol:.0f} vs средний {avg_vol:.0f} '
                      f'= {vol_ratio:.2f}x (нужно ≥ {STRUCT_VOLUME_RATIO}x)',
        })
        if not passed_3:
            blockers.append('пробой не подтверждён объёмом')

        passed_4 = current_price > avg_entry * 1.01
        checks.append({
            'name': 'цена_над_средней',
            'passed': passed_4,
            'detail': f'текущая цена {current_price:.6f} '
                      f'{"выше" if passed_4 else "не выше"} средней {avg_entry:.6f} более чем на 1%',
        })
        if not passed_4:
            blockers.append('цена уже вернулась к средней')

    metrics['structure'] = {
        'new_extreme_needed_pct': NEW_EXTREME_PCT,
        'confirm_candles': STRUCT_CONFIRM_CANDLES,
        'volume_ratio_needed': STRUCT_VOLUME_RATIO,
        'min_age_hours': STRUCT_MIN_HOURS,
    }
    return checks, blockers


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
    metrics: Optional[dict] = None,
) -> float:
    """Мягкие факторы: только score / reasons.
    Красный выставляется только через структурные проверки.
    """
    score = 0.5
    if metrics is None:
        metrics = {}

    # 1. Цена относительно средней 1h
    if ma_1h and ma_1h > 0:
        dist_ma = (current_price - ma_1h) / ma_1h * 100
        if dist_ma > 0:
            score += 0.15
            reasons.append(f'Цена выше MA1h (+{dist_ma:.1f}%)')
        else:
            score -= 0.15
            reasons.append(f'Цена ниже MA1h ({dist_ma:.1f}%)')

    # 2. Цена относительно средней 4h
    if ma_4h and ma_4h > 0:
        dist_ma4 = (current_price - ma_4h) / ma_4h * 100
        if dist_ma4 > -1.0:
            score += 0.08
            reasons.append(f'Цена выше MA4h ({dist_ma4:.1f}%)')
        else:
            score -= 0.10
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
            score += 0.10
            reasons.append(f'Бычьи свечи {bullish_candles}/{total_candles}')
        elif bull_ratio < 0.3:
            score -= 0.12
            reasons.append(f'Медвежьи свечи {total_candles - bullish_candles}/{total_candles}')

    # 4. Свечной объём (не пробойный! — пробойный проверяет структурный блок)
    if vol > 0 and len(klines_1h) >= 10:
        avg_vol = sum(float(k[5]) for k in klines_1h[-10:]) / 10
        if avg_vol > 0:
            vol_ratio = vol / avg_vol
            metrics['vol_ratio'] = round(vol_ratio, 2)
            if close_p > open_p and vol_ratio > 1.2:
                score += 0.08
                reasons.append(f'Объём выше среднего на растущей свече ({vol_ratio:.1f}x)')
            elif close_p < open_p and vol_ratio > 1.5:
                score -= 0.10
                reasons.append(f'Большой объём на падающей свече ({vol_ratio:.1f}x)')

    # 5. Тренд 4h — направление
    if len(klines_4h) >= 5:
        recent_4h = [float(k[4]) for k in klines_4h[-5:]]
        if recent_4h[-1] > recent_4h[0]:
            score += 0.08
            reasons.append('4h тренд восходящий')
        else:
            score -= 0.10
            reasons.append('4h тренд нисходящий')

    # 6. Текущая свеча — закрытие в верхней половине диапазона
    candle_range = high_p - low_p
    if candle_range > 0:
        close_pos = (close_p - low_p) / candle_range
        if close_pos > 0.6:
            score += 0.05
            reasons.append(f'Свеча закрылась в верхней части ({close_pos:.0%})')
        elif close_pos < 0.3:
            score -= 0.08
            reasons.append(f'Свеча закрылась в нижней части ({close_pos:.0%})')

    metrics['soft_score'] = round(score, 3)
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
    metrics: Optional[dict] = None,
) -> float:
    """Мягкие факторы для SHORT — зеркально LONG. Красный — только через структурные проверки."""
    score = 0.5
    if metrics is None:
        metrics = {}

    # 1. Цена относительно средней 1h
    if ma_1h and ma_1h > 0:
        dist_ma = (ma_1h - current_price) / ma_1h * 100
        if dist_ma > 0:
            score += 0.15
            reasons.append(f'Цена ниже MA1h (-{dist_ma:.1f}%)')
        else:
            score -= 0.15
            reasons.append(f'Цена выше MA1h ({dist_ma:.1f}%)')

    # 2. Цена относительно средней 4h
    if ma_4h and ma_4h > 0:
        dist_ma4 = (ma_4h - current_price) / ma_4h * 100
        if dist_ma4 > -1.0:
            score += 0.08
            reasons.append(f'Цена ниже MA4h ({dist_ma4:.1f}%)')
        else:
            score -= 0.10
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
            score += 0.10
            reasons.append(f'Медвежьи свечи {bearish_candles}/{total_candles}')
        elif bear_ratio < 0.3:
            score -= 0.12
            reasons.append(f'Бычьи свечи {total_candles - bearish_candles}/{total_candles}')

    # 4. Свечной объём (не пробойный! — пробойный проверяет структурный блок)
    if vol > 0 and len(klines_1h) >= 10:
        avg_vol = sum(float(k[5]) for k in klines_1h[-10:]) / 10
        if avg_vol > 0:
            vol_ratio = vol / avg_vol
            metrics['vol_ratio'] = round(vol_ratio, 2)
            if close_p < open_p and vol_ratio > 1.2:
                score += 0.08
                reasons.append(f'Объём выше среднего на падающей свече ({vol_ratio:.1f}x)')
            elif close_p > open_p and vol_ratio > 1.5:
                score -= 0.10
                reasons.append(f'Большой объём на растущей свече ({vol_ratio:.1f}x)')

    # 5. Тренд 4h — направление
    if len(klines_4h) >= 5:
        recent_4h = [float(k[4]) for k in klines_4h[-5:]]
        if recent_4h[-1] < recent_4h[0]:
            score += 0.08
            reasons.append('4h тренд нисходящий')
        else:
            score -= 0.10
            reasons.append('4h тренд восходящий')

    # 6. Текущая свеча — закрытие в нижней половине диапазона
    candle_range = high_p - low_p
    if candle_range > 0:
        close_pos = (close_p - low_p) / candle_range
        if close_pos < 0.4:
            score += 0.05
            reasons.append(f'Свеча закрылась в нижней части ({close_pos:.0%})')
        elif close_pos > 0.7:
            score -= 0.08
            reasons.append(f'Свеча закрылась в верхней части ({close_pos:.0%})')

    metrics['soft_score'] = round(score, 3)
    return min(1.0, max(0.0, score))
