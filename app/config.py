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

    # --- Детекция «ножа» (rejection candle) ---
    # Минимальный размер движения свечи (high-low)/open, в %, чтобы вообще
    # считать её кандидатом на нож. Мелкие свечи не рассматриваем.
    min_knife_move_pct: float = _f('MIN_KNIFE_MOVE_PCT', 2.0)
    # Тень (верхняя или нижняя) в % от диапазона свечи, при которой свеча
    # считается отвергнутой (rejection).
    rejection_shadow_pct: float = _f('REJECTION_SHADOW_PCT', 55.0)
    # Тело свечи не должно превышать эту долю диапазона, иначе это импульс,
    # а не отбой.
    max_body_frac: float = _f('MAX_BODY_FRAC', 0.35)
    # Суммарные тени в %, начиная с которых свеча «с тенью».
    min_shadow_pct: float = _f('MIN_SHADOW_PCT', 60.0)

    # --- Разделение всплеска и продолжения тренда ---
    # Минимальное суммарное движение окна старшего ТФ в %, чтобы назвать его трендом.
    trend_min_move_pct: float = _f('TREND_MIN_MOVE_PCT', 1.5)
    # Если сила тренда на старшем ТФ >= этого значения и направлена вдоль ножа —
    # считаем это продолжением тренда и входить нельзя.
    max_anchor_trend: float = _f('MAX_ANCHOR_TREND', 0.5)
    # Минимальная уверенность сигнала (0..1) для открытия сделки.
    min_confidence: float = _f('MIN_CONFIDENCE', 0.35)

    # --- Дополнительные фильтры входа (без противоречий с ножом) ---
    # Минимальный объём свечи-ножа к среднему за VOLUME_LOOKBACK, чтобы
    # назвать движение объёмным всплеском (а не тонким сквизом).
    min_knife_volume_ratio: float = _f('MIN_KNIFE_VOLUME_RATIO', 1.2)
    # Минимальный отрыв текущей цены от экстремума ножа в %, подтверждающий
    # что отскок уже начался (ушли от «падающего ножа»). Порог строже,
    # чем фиксированный множитель 1.001 в classify_event.
    min_bounce_pct: float = _f('MIN_BOUNCE_PCT', 0.5)
    # Максимально допустимый возврат к открытию свечи в % от движения ножа.
    # Если цена уже отскочила почти к open — большая часть движения съедена,
    # матожидание входа худшее.
    max_retrace_to_open_pct: float = _f('MAX_RETRACE_TO_OPEN_PCT', 70.0)

    # --- Комиссии Bybit (USDT perpetual, taker) ---
    # Bybit берёт комиссию с НОМИНАЛА позиции и отдельно на вход и на выход.
    taker_fee_pct: float = _f('TAKER_FEE_PCT', 0.055)


settings = Settings()
