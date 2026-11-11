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


@dataclass(frozen=True)
class Settings:
    threshold: float = _f('THRESHOLD', 0.5)
    window_minutes: int = _i('WINDOW_MINUTES', 60)
    ma_period: int = _i('MA_PERIOD', 24)
    min_max_leverage: int = _i('MIN_MAX_LEVERAGE', 10)
    max_alerts: int = _i('MAX_ALERTS', 200)
    symbols_refresh_sec: int = _i('SYMBOLS_REFRESH_SEC', 3600)
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')


settings = Settings()
