"""Три режима торговли: скальпинг / интрадэй / долгосрок.

Каждый режим задаёт свой рабочий ТФ (на котором ищем пин-бар), якорный ТФ
(по нему определяем тренд — вход ТОЛЬКО по тренду якоря) и заранее
подобранные параметры входа/сетки/выходов. Порог уверенности растёт по
мере уменьшения рабочего ТФ: чем ниже ТФ, тем больше шума, тем сильнее
должен быть контекст, чтобы сделка стоила комиссий.

Поля *_pct выражены в % от цены, *_atr — в единицах ATR(14) рабочего ТФ.
Если задано и то и другое, для расстояния берётся максимум из них: на
спокойном рынке сетка не «шумит», на волатильном — не стоит слишком
плотно к экстремумам.
"""

from dataclasses import dataclass, replace as dc_replace
from typing import Dict, Optional


@dataclass(frozen=True)
class ModeParams:
    id: str
    label: str
    # --- Таймфреймы ---
    work_tf: str            # Bybit interval, где ищем пин-бар ('5', '60', 'D')
    work_label: str         # человекочитаемый ('5m', '1H', '1D')
    anchor_tf: str          # Bybit interval трендового фильтра
    anchor_label: str       # человекочитаемый ('1h', '1d', '1w')
    confirm_window_sec: int # сколько секунд ждём подтверждения после пин-бара
    scan_period_sec: int    # минимальный период прохода по символам
    # --- Пороги сигнала ---
    min_confidence: float   # нижняя граница уверенности (0.6..0.8 по ТФ)
    pin_shadow_frac: float  # минимальная доля тени от диапазона свечи (2/3+)
    move_atr_mult: float    # мин. диапазон свечи как множитель ATR(work_tf)
    move_pct_min: float     # мин. диапазон свечи в % (защита тихих рынков)
    shadow_lookback_atr: float  # тень пин-бара должна достать уровень в N*ATR
    space_rr_min: float     # минимум (расстояние до цели) / (до entry-stop)
    # --- Управление позицией (перезаписывает глобальные настройки) ---
    grid_step_pct: Optional[float] = None
    grid_step_atr: Optional[float] = None
    grid_step_multiplier: float = 1.0
    initial_exit_pct: Optional[float] = None
    exit_step_atr: Optional[float] = None
    exit_step_multiplier: float = 1.0
    max_steps: int = 4
    hold_max_hours: int = 96


MODES: Dict[str, ModeParams] = {
    'scalp': ModeParams(
        id='scalp',
        label='Скальпинг (5m / якорь 1H)',
        work_tf='5',
        work_label='5m',
        anchor_tf='60',
        anchor_label='1H',
        confirm_window_sec=600,        # ~2 свечи 5m
        scan_period_sec=20,
        min_confidence=0.80,           # низкий ТФ -> максимальная планка
        pin_shadow_frac=0.70,
        move_atr_mult=1.6,
        move_pct_min=0.45,
        shadow_lookback_atr=0.8,
        space_rr_min=1.5,
        grid_step_pct=0.55,
        grid_step_atr=0.8,
        grid_step_multiplier=1.4,
        initial_exit_pct=1.1,
        exit_step_atr=1.2,
        exit_step_multiplier=1.3,
        max_steps=3,
        hold_max_hours=24,
    ),
    'intraday': ModeParams(
        id='intraday',
        label='Интрадэй (15m / якорь 1D)',
        work_tf='15',
        work_label='15m',
        anchor_tf='D',
        anchor_label='1D',
        confirm_window_sec=1800,       # ~2 свечи 15m
        scan_period_sec=60,
        min_confidence=0.70,
        pin_shadow_frac=0.66,
        move_atr_mult=1.4,
        move_pct_min=0.9,
        shadow_lookback_atr=1.0,
        space_rr_min=1.5,
        grid_step_pct=1.3,
        grid_step_atr=1.0,
        grid_step_multiplier=1.6,
        initial_exit_pct=2.6,
        exit_step_atr=1.5,
        exit_step_multiplier=1.4,
        max_steps=4,
        hold_max_hours=72,
    ),
    'swing': ModeParams(
        id='swing',
        label='Долгосрок (4H / якорь 1W)',
        work_tf='240',
        work_label='4H',
        anchor_tf='W',
        anchor_label='1W',
        confirm_window_sec=7200,       # ~2 свечи 4H
        scan_period_sec=300,
        min_confidence=0.60,           # старший ТФ -> меньше шума
        pin_shadow_frac=0.62,
        move_atr_mult=1.2,
        move_pct_min=2.0,
        shadow_lookback_atr=1.2,
        space_rr_min=1.5,
        grid_step_pct=3.5,
        grid_step_atr=1.2,
        grid_step_multiplier=2.0,
        initial_exit_pct=9.0,
        exit_step_atr=2.0,
        exit_step_multiplier=1.6,
        max_steps=6,
        hold_max_hours=672,            # 28 дней
    ),
}

DEFAULT_MODE_ID = 'intraday'


def get_mode(mode_id: Optional[str]) -> ModeParams:
    """Возвращает параметры режима; неизвестный/пустой id -> дефолт."""
    if mode_id and mode_id in MODES:
        return MODES[mode_id]
    return MODES[DEFAULT_MODE_ID]


# Поле Settings/Trade, хранящее режим сделки. В БД — просто строка id.
VALID_MODE_IDS = tuple(MODES.keys())


def mode_or_default(mode_id: Optional[str]) -> ModeParams:
    return get_mode(mode_id)


def apply_mode_defaults(params: ModeParams, **overrides) -> dict:
    """Собирает словарь параметров сделки: override (конфиг монеты или
    глобальные настройки) > режим > системный дефолт.

    usage: apply_mode_defaults(mode, grid_step_pct=user_grid_step)
    """
    out = {}
    for k, v in overrides.items():
        out[k] = v
    return out


def resolve_distance(pct_value: Optional[float], atr_value: Optional[float],
                     atr_mult: Optional[float], price: float) -> float:
    """Расстояние в %: максимум из фиксированного порога и ATR-привязки."""
    pct = pct_value or 0.0
    if atr_value and atr_mult and price > 0:
        pct = max(pct, atr_value * atr_mult / price * 100.0)
    return pct


__all__ = ['ModeParams', 'MODES', 'DEFAULT_MODE_ID', 'get_mode',
           'mode_or_default', 'apply_mode_defaults', 'resolve_distance',
           'VALID_MODE_IDS']
