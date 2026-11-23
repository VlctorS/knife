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


# Секунды в одном баре интервала Bybit. Используется для «живого» якоря:
# пока старшая свеча не закрылась, её направление считаем по накопленному
# объёму младших баров внутри неё (не ждём закрытия 1H/4H/D).
_INTERVAL_SEC = {
    '1': 60, '3': 180, '5': 300, '15': 900, '30': 1800,
    '60': 3600, '120': 7200, '240': 14400, '360': 21600,
    'D': 86400, 'W': 604800, 'M': 2592000,
}

# Интервалы якорей, которые НЕ нужно ждать по полному закрытию бара:
# 1H-якорь скальпа закрывается 60 мин — это вдвое больше confirm_window,
# сигнал успевает протухнуть. Якорь 1D закрывается сутки — неприемлемо
# ни для какого режима. Поэтому тренд якоря считается «накопленным» баром.
def interval_sec(tf) -> int:
    """Длительность одного бара интервала Bybit в секундах."""
    return _INTERVAL_SEC.get(str(tf), 3600)


def anchor_refresh_cap(mode_id: str, work_tf: str) -> int:
    """Свежесть данных якорного ТФ для режима (сек)."""
    return interval_sec(work_tf) * 2


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
    # --- Параметры монитора (памп/дамп) — автоматически применяются при выборе режима ---
    monitor_threshold: float = 0.5      # мин. изменение цены в % для алерта монитора
    monitor_window_minutes: int = 60    # окно сравнения для монитора в минутах
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
        # Якорь — НАКОПЛЕННЫЙ часовой бар из 5m-свечей: не ждём закрытия
        # 1H (60 мин > confirm_window), тренд часа считаем живым объёмом.
        anchor_tf='60',
        anchor_label='1H (живой)',
        monitor_threshold=0.3,           # более чувствительный для быстрых движений
        monitor_window_minutes=5,        # 5 минут
        confirm_window_sec=900,          # ~3 свечи 5m — даём время на подтверждение
        scan_period_sec=20,
        min_confidence=0.40,             # скальп 5m — шумный, но 0.40 достаточно с ATR-фильтрами
        pin_shadow_frac=0.55,            # 0.55 для 5m — не жёстко 0.70
        move_atr_mult=0.8,               # 0.8 ATR для 5m — не 1.6, слишком высоко
        move_pct_min=0.2,                # 0.2% для 5m — не 0.45%, слишком много
        shadow_lookback_atr=0.5,         # допуск контакта тени с уровнем: 0.5*ATR
        space_rr_min=1.2,                # 1.2 для скальпа достаточно (комиссии съедают RR)
        grid_step_pct=0.5,
        grid_step_atr=0.7,
        grid_step_multiplier=1.3,
        initial_exit_pct=0.7,            # цель выхода — 0.7% для быстрого скальпа
        exit_step_atr=1.0,
        exit_step_multiplier=1.2,
        max_steps=5,                     # 5 шагов сетки для 5m — больше возможностей
        hold_max_hours=6,                # 6ч макс для скальпа — быстро закрываем
    ),
    'intraday': ModeParams(
        id='intraday',
        label='Интрадэй (15m / якорь 1H)',
        work_tf='15',
        work_label='15m',
        # Якорь 1D был ошибкой: дневная свеча закрывается сутки — сигнал
        # умирал раньше. Интрадэй торгуется ВНУТРИ дня, поэтому якорь —
        # живой часовой тренд, а день остаётся контекстом (SR/уровни).
        anchor_tf='60',
        anchor_label='1H (живой)',
        monitor_threshold=0.5,           # стандартный
        monitor_window_minutes=60,       # 1 час
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
        label='Долгосрок (4H / якорь 1D)',
        work_tf='240',
        work_label='4H',
        # Единственный режим, где якорь 1D оправдан: сделка живут дни,
        # ожидание закрытия дневного бара = часть цикла сделки.
        anchor_tf='D',
        anchor_label='1D',
        monitor_threshold=1.0,           # фильтр шума — только сильные движения
        monitor_window_minutes=240,      # 4 часа
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


def get_mode(mode_id: Optional[str] = None) -> ModeParams:
    """Возвращает параметры режима.

    Если mode_id передан и валиден — возвращает этот режим.
    Если mode_id не передан (None) — возвращает DEFAULT_MODE_ID.
    """
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
