import os
from dataclasses import dataclass

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


# =============================================================================
# Базовые (env) параметры — fallback для всех режимов
# =============================================================================

@dataclass(frozen=True)
class BaseSettings:
    threshold: float = _f('THRESHOLD', 0.5)
    window_minutes: int = _i('WINDOW_MINUTES', 60)
    ma_period: int = _i('MA_PERIOD', 24)
    min_max_leverage: int = _i('MIN_MAX_LEVERAGE', 10)
    max_alerts: int = _i('MAX_ALERTS', 200)
    symbols_refresh_sec: int = _i('SYMBOLS_REFRESH_SEC', 3600)
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')

    # --- Детекция «ножа» (rejection candle) ---
    min_knife_move_pct: float = _f('MIN_KNIFE_MOVE_PCT', 2.0)
    rejection_shadow_pct: float = _f('REJECTION_SHADOW_PCT', 55.0)
    max_body_frac: float = _f('MAX_BODY_FRAC', 0.35)
    min_shadow_pct: float = _f('MIN_SHADOW_PCT', 60.0)

    # --- Разделение всплеска и продолжения тренда ---
    trend_min_move_pct: float = _f('TREND_MIN_MOVE_PCT', 1.5)
    max_anchor_trend: float = _f('MAX_ANCHOR_TREND', 0.5)
    min_confidence: float = _f('MIN_CONFIDENCE', 0.35)

    # --- Дополнительные фильтры входа ---
    min_knife_volume_ratio: float = _f('MIN_KNIFE_VOLUME_RATIO', 1.2)
    min_bounce_pct: float = _f('MIN_BOUNCE_PCT', 0.5)
    max_retrace_to_open_pct: float = _f('MAX_RETRACE_TO_OPEN_PCT', 70.0)

    # --- Комиссии ---
    taker_fee_pct: float = _f('TAKER_FEE_PCT', 0.055)


# =============================================================================
# Динамический accessor — объединяет BaseSettings + текущий trade_mode
# =============================================================================

class _LazySettings:
    """Обёртка: по умолчанию возвращает base, но для threshold и
    window_minutes динамически читает trade_mode из БД и подменяет
    значениями режима.

    Механизм ленивый: сессия БД открывается только при обращении к
    threshold/window_minutes, не при каждом экспорте config.
    """
    __slots__ = ('_base',)

    def __init__(self) -> None:
        self._base = BaseSettings()

    def __getattr__(self, name: str):
        base = object.__getattribute__(self, '_base')
        return getattr(base, name)

    @property
    def threshold(self) -> float:
        v = self._try_mode('monitor_threshold')
        return v if v is not None else self._base.threshold

    @property
    def window_minutes(self) -> int:
        v = self._try_mode('monitor_window_minutes')
        return v if v is not None else self._base.window_minutes

    @staticmethod
    def _try_mode(attr: str):
        """Пытаемся прочитать attr из ModeParams текущего режима."""
        try:
            from .modes import get_mode
            mode = get_mode()
            val = getattr(mode, attr, None)
            return val
        except Exception:
            return None


settings = _LazySettings()
