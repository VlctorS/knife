import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
load_dotenv(os.path.join(_ROOT, '.env'))
load_dotenv()


def _f(name, default):
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def _i(name, default):
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class BaseSettings:
    """Static fallback для monitor.py."""
    threshold: float = _f('THRESHOLD', 0.5)
    window_minutes: int = _i('WINDOW_MINUTES', 60)
    ma_period: int = _i('MA_PERIOD', 24)
    min_max_leverage: int = _i('MIN_MAX_LEVERAGE', 10)
    max_alerts: int = _i('MAX_ALERTS', 200)
    symbols_refresh_sec: int = _i('SYMBOLS_REFRESH_SEC', 3600)
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')

    # --- Детекция «ножа» ---
    min_knife_move_pct: float = _f('MIN_KNIFE_MOVE_PCT', 2.0)
    rejection_shadow_pct: float = _f('REJECTION_SHADOW_PCT', 55.0)
    max_body_frac: float = _f('MAX_BODY_FRAC', 0.35)
    min_shadow_pct: float = _f('MIN_SHADOW_PCT', 60.0)

    trend_min_move_pct: float = _f('TREND_MIN_MOVE_PCT', 1.5)
    max_anchor_trend: float = _f('MAX_ANCHOR_TREND', 0.5)
    min_confidence: float = _f('MIN_CONFIDENCE', 0.35)

    min_knife_volume_ratio: float = _f('MIN_KNIFE_VOLUME_RATIO', 1.2)
    min_bounce_pct: float = _f('MIN_BOUNCE_PCT', 0.5)
    max_retrace_to_open_pct: float = _f('MAX_RETRACE_TO_OPEN_PCT', 70.0)

    taker_fee_pct: float = _f('TAKER_FEE_PCT', 0.055)


# =============================================================================
# Текущие параметры монитора — обновляются при смене режима торговли.
# monitor.py читает monitor_threshold и monitor_window_minutes
# напрямую из этого модуля.
# =============================================================================

_monitor_threshold: float = 0.5
_monitor_window_minutes: int = 60


def update_monitor_params(mode_id: Optional[str]) -> None:
    """Устанавливает параметры монитора на основе выбранного режима.

    Если mode_id не передан или невалиден — использует fallback из env.
    """
    global _monitor_threshold, _monitor_window_minutes
    try:
        from .modes import MODES
        if mode_id and mode_id in MODES:
            m = MODES[mode_id]
            _monitor_threshold = m.monitor_threshold
            _monitor_window_minutes = m.monitor_window_minutes
        else:
            base = BaseSettings()
            _monitor_threshold = base.threshold
            _monitor_window_minutes = base.window_minutes
    except Exception:
        base = BaseSettings()
        _monitor_threshold = base.threshold
        _monitor_window_minutes = base.window_minutes


def get_monitor_threshold() -> float:
    return _monitor_threshold


def get_monitor_window_minutes() -> int:
    return _monitor_window_minutes


class _SettingsAccessor:
    """Proxy для backward compatibility: monitor.py использует settings.threshold."""
    def __getattr__(self, name: str):
        base = BaseSettings()
        if name == 'threshold':
            return get_monitor_threshold()
        if name == 'window_minutes':
            return get_monitor_window_minutes()
        return getattr(base, name)


settings = _SettingsAccessor()
