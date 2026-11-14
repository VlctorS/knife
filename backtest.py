"""Движок бэктеста ножевого бота (CROSS MARGIN).

Импортируйте run_backtest(**params) для запуска из кода.
Запуск CLI: python backtest.py --days 90 --balance 30
"""

import argparse
import asyncio
import json
import logging
import os
import statistics as st_
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import aiohttp

from app.bybit import BybitREST
from app.config import settings
from app.state import classify_event, moving_average

logging.basicConfig(level=logging.ERROR)

MIN_POS_SIZE = 6.0
VOLUME_LOOKBACK = 20
MM_RATE_DEFAULT = 0.005

_MS = {'15': 900_000, '60': 3_600_000, '240': 14_400_000, 'D': 86_400_000}


def _pick(cfg_val, global_val, fallback):
    if cfg_val and cfg_val > 0:
        return cfg_val
    if global_val and global_val > 0:
        return global_val
    return fallback


# ================================================================ загрузка

async def fetch_klines(rest, symbol, interval, start_ms, end_ms):
    out = []
    cursor_end = end_ms
    step = _MS[interval]
    pages = 0
    while cursor_end > start_ms and pages < 12:
        limit = min(1000, max(2, (cursor_end - start_ms) // step + 2))
        params = {'category': 'linear', 'symbol': symbol, 'interval': interval,
                  'end': str(cursor_end), 'limit': str(int(limit))}
        data = await rest._get('/v5/market/kline', params)
        lst = data.get('result', {}).get('list', []) or []
        if not lst:
            break
        lst.reverse()
        if lst and out and lst[-1][0] >= out[0][0]:
            lst = [k for k in lst if k[0] < out[0][0]]
        if not lst:
            break
        out = lst + out
        cursor_end = int(out[0][0]) - 1
        pages += 1
        await asyncio.sleep(0.03)
    return [k for k in out if int(k[0]) >= start_ms]


async def fetch_risk_limits(rest, symbols):
    out = {}
    cursor = ''
    pages = 0
    while pages < 40:
        params = {'category': 'linear', 'limit': '1000'}
        if cursor:
            params['cursor'] = cursor
        data = await rest._get('/v5/market/risk-limit', params)
        if data.get('retCode') != 0:
            break
        result = data.get('result', {})
        for it in result.get('list', []):
            sym = it.get('symbol')
            if sym in symbols:
                try:
                    out[sym] = float(it['maintenanceMargin'])
                except (TypeError, ValueError, KeyError):
                    pass
        cursor = result.get('nextPageCursor') or ''
        if not cursor:
            break
        pages += 1
    return out


def _mm_rate(sym, risk_limits):
    if sym in risk_limits and risk_limits[sym] > 0:
        return risk_limits[sym]
    return MM_RATE_DEFAULT


# ============================================================= пороги

def symbol_threshold(kl_4h):
    prostrels = []
    for k in kl_4h[:-1]:
        h, l, v = float(k[2]), float(k[3]), float(k[5])
        if l > 0 and v > 0:
            prostrels.append((h - l) / l * 100)
    if not prostrels:
        return max(3.0, settings.min_knife_move_pct)
    avg_p = sum(prostrels) / len(prostrels)
    threshold = max(3.0, avg_p * 0.85)
    return max(threshold, settings.min_knife_move_pct)


# ============================================================= cross broker

class CrossPosition:
    __slots__ = ('sym', 'side', 'lev', 'qty', 'avg', 'entry_fee',
                 'grid_step', 'exit_step', 'max_steps', 'opened_ts',
                 'adds', 'partials', 'last_price', 'last_ts')

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k, 0))


class CrossBroker:
    def __init__(self, balance, fee_pct):
        self.balance = balance
        self.fee = fee_pct / 100.0
        self.gross_fee_paid = 0.0
        self.positions = []

    def unrealized_pnl(self):
        total = 0.0
        for p in self.positions:
            gross = p.qty * (p.last_price - p.avg) if p.side == 'LONG' \
                else p.qty * (p.avg - p.last_price)
            total += gross - p.entry_fee
        return total

    def equity(self):
        return self.balance + self.unrealized_pnl()

    def maintenance_margin(self, risk_limits):
        total = 0.0
        for p in self.positions:
            mmr = _mm_rate(p.sym, risk_limits)
            total += abs(p.qty * p.last_price) * mmr
        return total

    def is_liquidated(self, risk_limits):
        eq = self.equity()
        mm = self.maintenance_margin(risk_limits)
        return mm > 0 and eq <= mm

    def free_margin(self, risk_limits):
        im = sum(abs(p.qty * p.avg) / p.lev for p in self.positions)
        return self.equity() - im

    def can_open(self, size, lev, risk_limits):
        need = size / lev + size * self.fee
        fm = self.free_margin(risk_limits)
        return fm >= need and self.equity() > 0

    def open(self, sym, side, price, size, lev, ts, max_steps):
        fee = size * self.fee
        self.balance -= fee
        self.gross_fee_paid += fee
        qty = size / price
        p = CrossPosition(sym=sym, side=side, lev=lev, qty=qty, avg=price,
                          entry_fee=fee, grid_step=0, exit_step=0,
                          max_steps=max_steps, opened_ts=ts, adds=0,
                          partials=0, last_price=price, last_ts=ts)
        self.positions.append(p)
        return p

    def add_grid(self, p, price, size):
        fee = size * self.fee
        self.balance -= fee
        self.gross_fee_paid += fee
        add_qty = size / price
        old_qty = p.qty
        p.qty = old_qty + add_qty
        p.avg = (p.avg * old_qty + price * add_qty) / p.qty
        p.entry_fee += fee
        p.grid_step += 1
        p.adds += 1

    def close_frac(self, p, price, frac, ts, why):
        closed_qty = p.qty * frac
        gross = closed_qty * (price - p.avg) if p.side == 'LONG' \
            else closed_qty * (p.avg - price)
        exit_fee = closed_qty * price * self.fee
        entry_part = p.entry_fee * frac
        pnl = gross - exit_fee - entry_part
        self.balance += pnl
        self.gross_fee_paid += exit_fee
        p.qty -= closed_qty
        p.entry_fee -= entry_part
        done = p.qty <= 1e-12 or p.qty * price < 1.0 or frac >= 0.999
        if done:
            if p.qty > 1e-12:
                tail = p.qty
                tg = tail * (price - p.avg) if p.side == 'LONG' else tail * (p.avg - price)
                te = tail * price * self.fee
                tp = tg - te - p.entry_fee
                self.balance += tp
                p.qty = 0.0
                p.entry_fee = 0.0
            self.positions.remove(p)
        else:
            p.avg = price
            p.exit_step += 1
            p.grid_step = 0
            p.partials += 1
        return pnl, done

    def liquidate_all(self, ts):
        losses = []
        for p in self.positions:
            px = p.last_price
            gross = p.qty * (px - p.avg) if p.side == 'LONG' else p.qty * (p.avg - px)
            pnl = gross - p.entry_fee
            losses.append(dict(sym=p.sym, side=p.side, opened=p.opened_ts,
                               closed=ts, exit=px, pnl=pnl, adds=p.adds,
                               partials=p.partials, liq=True, why='PORTFOLIO_LIQ'))
        self.balance = 0.0
        self.positions.clear()
        return losses


# ============================================================= симуляция

def _avg_volume(kl, lookback=VOLUME_LOOKBACK):
    if len(kl) < lookback + 1:
        return 0.0
    prev = kl[-(lookback + 1):-1]
    vols = [float(k[5]) for k in prev if float(k[5]) > 0]
    return sum(vols) / len(vols) if vols else 0.0


def _eval_entry(knife_tf, anchor_tf, price, sym, threshold, stats, seen, candle_id, min_conf):
    last_closed = knife_tf[-2]
    o = float(last_closed[1])
    v = float(last_closed[5])
    if o <= 0 or v <= 0:
        return None
    av = _avg_volume(knife_tf)
    if av <= 0 or v < av:
        stats['skip_lowvol'] += 1
        return None
    change = (float(last_closed[4]) - o) / o * 100
    if abs(change) < threshold:
        return None
    ma_knife = moving_average(knife_tf, min(settings.ma_period, 20))
    ma_anchor = moving_average(anchor_tf, min(settings.ma_period, 20)) if len(anchor_tf) >= 4 else None
    ev = classify_event(knife_tf, anchor_tf, price, ma_knife, ma_anchor)
    if not ev['is_spike']:
        stats['skip_not_spike'] += 1
        seen.add(candle_id)
        return None
    if ev['confidence'] < min_conf:
        stats['skip_lowconf'] += 1
        seen.add(candle_id)
        return None
    seen.add(candle_id)
    return 'LONG' if ev['direction'] == 'long' else 'SHORT'


def _closed_prefix(kl, ts):
    lo, hi = 0, len(kl)
    while lo < hi and int(kl[hi - 1][0]) >= ts:
        hi -= 1
    return kl[:hi + 1]


def run_backtest(balance, days, symbols_list=None, min_turnover=5.0,
                 min_leverage=10, fee_pct=0.055,
                 entry_size=6.0, grid_step_pct=5.0, grid_step_mult=2.5,
                 grid_size_mult=1.0, max_steps=10,
                 initial_exit_pct=15.0, exit_step_mult=1.6,
                 exit_contract_pct=50.0, min_confidence=0.4,
                 position_size_grid=None):
    """Запуск бэктеста. Возвращает dict с результатами.

    Параметры:
        balance: начальный баланс USDT
        days: период в днях
        symbols_list: список символов для теста (None = все из Bybit)
        min_turnover: мин. оборот 24ч в млн
        min_leverage: мин. макс. плечо
        fee_pct: комиссия taker %
        entry_size: размер позиции при входе
        grid_step_pct: первый шаг сетки %
        grid_step_mult: множитель шага сетки
        grid_size_mult: множитель размера сетки
        max_steps: макс. шагов сетки
        initial_exit_pct: первый выход %
        exit_step_mult: множитель выхода
        exit_contract_pct: % контракта при выходе
        min_confidence: мин. уверенность сигнала
        position_size_grid: размер сетки (None = entry_size)
    """
    if position_size_grid is None:
        position_size_grid = entry_size

    global GS_MIN_CONF
    GS_MIN_CONF = min_confidence

    end_ms = int(time.time() * 1000)
    end_ms -= end_ms % _MS['15']
    start_ms = end_ms - days * 86_400_000
    warmup_ms = 40 * 86_400_000
    gs = dict(grid_step_pct=grid_step_pct, grid_step_multiplier=grid_step_mult,
              grid_size_multiplier=grid_size_mult, max_steps=max_steps,
              position_size_entry=entry_size, position_size_grid=position_size_grid,
              initial_exit_pct=initial_exit_pct, exit_step_multiplier=exit_step_mult,
              exit_contract_pct=exit_contract_pct)

    async def _run():
        async with aiohttp.ClientSession() as s:
            rest = BybitREST(s)
            if symbols_list:
                # загружаем только указанные
                lev_map = await rest.get_max_leverage_map()
                tickers = await rest.get_tickers()
                turn = {t['symbol']: float(t.get('turnover24h') or 0) for t in tickers}
                symbols = []
                for sym in symbols_list:
                    ml = lev_map.get(sym)
                    if ml is None or ml < min_leverage:
                        continue
                    if turn.get(sym, 0) / 1e6 < min_turnover:
                        continue
                    symbols.append((sym, ml))
            else:
                instr = await rest.get_instruments()
                lev_map = await rest.get_max_leverage_map()
                tickers = await rest.get_tickers()
                turn = {t['symbol']: float(t.get('turnover24h') or 0) for t in tickers}
                symbols = []
                for it in instr:
                    sym = it['symbol']
                    ml = lev_map.get(sym)
                    if ml is None or ml < min_leverage:
                        continue
                    if turn.get(sym, 0) / 1e6 < min_turnover:
                        continue
                    symbols.append((sym, ml))
                symbols.sort()

            sem = asyncio.Semaphore(5)
            async def load(sym):
                async with sem:
                    try:
                        kl15 = await fetch_klines(rest, sym, '15', start_ms - 5*86_400_000, end_ms)
                        kl4 = await fetch_klines(rest, sym, '240', start_ms - warmup_ms, end_ms)
                        kld = await fetch_klines(rest, sym, 'D', start_ms - warmup_ms, end_ms)
                        if len(kl15) < 200 or len(kl4) < 60 or len(kld) < 30:
                            return sym, None
                        return sym, dict(kl15=kl15, kl4=kl4, kld=kld)
                    except Exception:
                        return sym, None

            res = await asyncio.gather(*(load(x) for x, _ in symbols))
            lev_dict = dict(symbols)
            data = {}
            for sym, d in res:
                if d:
                    d['lev'] = min(float(lev_dict[sym]), 125.0)
                    data[sym] = d

            risk_limits = await fetch_risk_limits(rest, set(data.keys()))
            threshold_cache = {sym: symbol_threshold(d['kl4']) for sym, d in data.items()}

        broker = CrossBroker(balance, fee_pct)
        stats = dict(entries=0, liqs=0, tps=0, skip_no_margin=0, skip_lowvol=0,
                     skip_not_spike=0, skip_lowconf=0)

        trades, curve, bankrupt_at = simulate(data, gs, broker, start_ms, end_ms,
                                              stats, risk_limits, threshold_cache)

        return _build_report(balance, days, broker, trades, curve, data, stats,
                             bankrupt_at, len(data))

    return asyncio.run(_run())


def simulate(data, gs, broker, start_ms, end_ms, stats, risk_limits=None,
             threshold_cache=None):
    risk_limits = risk_limits or {}
    threshold_cache = threshold_cache or {}
    syms = sorted(data.keys())
    ptr = {s: 0 for s in syms}
    seen_entry = set()
    trades = []
    curve = []
    peak_equity = broker.balance
    pos_by_sym = {}
    for p in broker.positions:
        pos_by_sym[p.sym] = p

    ts = start_ms + _MS['15']
    guard = 0
    bankrupt_at = None

    while ts <= end_ms and guard < 9000:
        guard += 1
        for sym in syms:
            d = data[sym]
            kl15 = d['kl15']
            advanced = False
            while ptr[sym] < len(kl15) and int(kl15[ptr[sym]][0]) + _MS['15'] <= ts:
                ptr[sym] += 1
                advanced = True
            if not advanced:
                continue
            i = ptr[sym]
            if i == 0 or i > len(kl15):
                continue
            c = kl15[i - 1]
            o15, h15, l15, c15 = float(c[1]), float(c[2]), float(c[3]), float(c[4])
            price = c15

            p = pos_by_sym.get(sym)
            if p is not None:
                p.last_price, p.last_ts = price, ts
                pref15 = _closed_prefix(kl15, ts)
                ma15 = moving_average(pref15, 20) if len(pref15) >= 21 else None

                if p.grid_step < p.max_steps and ma15:
                    start_pct = _pick(None, gs.get('grid_step_pct'), 5.0)
                    mult = _pick(None, gs.get('grid_step_multiplier'), 2.5)
                    step_pct = start_pct * (mult ** p.grid_step)
                    can_grid = ((p.side == 'LONG' and price <= p.avg * (1 - step_pct / 100) and price < ma15) or
                                (p.side == 'SHORT' and price >= p.avg * (1 + step_pct / 100) and price > ma15))
                    if can_grid:
                        gsize = _pick(None, gs.get('position_size_grid'), MIN_POS_SIZE)
                        gm = _pick(None, gs.get('grid_size_multiplier'), 1.0)
                        if gm > 1.0:
                            gsize *= (gm ** p.grid_step)
                        gsize = max(MIN_POS_SIZE, gsize)
                        if broker.can_open(gsize, p.lev, risk_limits):
                            broker.add_grid(p, price, gsize)
                        else:
                            stats['skip_no_margin'] += 1

                chg = (price - p.avg) / p.avg * 100 if p.side == 'LONG' \
                    else (p.avg - price) / p.avg * 100
                start_exit = _pick(None, gs.get('initial_exit_pct'), 15.0)
                exit_mult = _pick(None, gs.get('exit_step_multiplier'), 1.6)
                close_pct = _pick(None, gs.get('exit_contract_pct'), 50.0)
                target = start_exit * (exit_mult ** p.exit_step)
                if chg >= target:
                    full = (p.qty * (close_pct / 100) * price < 1.0) or \
                           (p.qty * (1 - close_pct / 100) < 0.001)
                    frac = 1.0 if full else close_pct / 100
                    pnl, done = broker.close_frac(p, price, frac, ts, 'TP')
                    if done:
                        trades.append(dict(sym=p.sym, side=p.side, opened=p.opened_ts,
                                           closed=ts, exit=price, pnl=pnl,
                                           adds=p.adds, partials=p.partials,
                                           liq=False, why='TP'))
                        pos_by_sym.pop(sym, None)
                        stats['tps'] += 1

            if p is None and i % 16 == 0:
                pref4 = _closed_prefix(d['kl4'], ts)
                prefd = _closed_prefix(d['kld'], ts)
                if len(pref4) >= 25:
                    candle_id = f'{sym}_240_{pref4[-2][0]}'
                    if candle_id not in seen_entry:
                        ok = _eval_entry(pref4, prefd, price, sym,
                                         threshold_cache.get(sym, 3.0),
                                         stats, seen_entry, candle_id, GS_MIN_CONF)
                        if ok and broker.can_open(gs.get('position_size_entry', MIN_POS_SIZE),
                                                   data[sym]['lev'], risk_limits):
                            pos = broker.open(sym, ok, price,
                                              gs.get('position_size_entry', MIN_POS_SIZE),
                                              data[sym]['lev'], ts,
                                              int(_pick(None, gs.get('max_steps'), 10)))
                            pos_by_sym[sym] = pos
                            stats['entries'] += 1
                            continue
                if len(prefd) >= 25:
                    candle_id_d = f'{sym}_D_{prefd[-2][0]}'
                    if candle_id_d not in seen_entry:
                        ok = _eval_entry(prefd, pref4, price, sym,
                                         threshold_cache.get(sym, 3.0),
                                         stats, seen_entry, candle_id_d, GS_MIN_CONF)
                        if ok and broker.can_open(gs.get('position_size_entry', MIN_POS_SIZE),
                                                   data[sym]['lev'], risk_limits):
                            pos = broker.open(sym, ok, price,
                                              gs.get('position_size_entry', MIN_POS_SIZE),
                                              data[sym]['lev'], ts,
                                              int(_pick(None, gs.get('max_steps'), 10)))
                            pos_by_sym[sym] = pos
                            stats['entries'] += 1

            if broker.positions and broker.is_liquidated(risk_limits):
                losses = broker.liquidate_all(ts)
                for l in losses:
                    trades.append(l)
                    stats['liqs'] += 1
                pos_by_sym.clear()
                if bankrupt_at is None:
                    bankrupt_at = ts

            eq = broker.equity()
            peak_equity = max(peak_equity, eq)
            curve.append((ts, round(eq, 4)))
            ts += _MS['15']

    for p in list(broker.positions):
        pnl = p.qty * (p.last_price - p.avg) if p.side == 'LONG' \
            else p.qty * (p.avg - p.last_price)
        pnl -= p.entry_fee
        broker.balance += pnl
        trades.append(dict(sym=p.sym, side=p.side, opened=p.opened_ts,
                           closed=p.last_ts, exit=p.last_price, pnl=pnl,
                           adds=p.adds, partials=p.partials, liq=False,
                           why='EOD', still_open=True))
    broker.positions.clear()

    return trades, curve, bankrupt_at


def _build_report(balance, days, broker, trades, curve, data, stats, bankrupt_at, n_symbols):
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    liqs = [t for t in trades if t['liq']]
    pf_g = sum(t['pnl'] for t in wins)
    pf_l = -sum(t['pnl'] for t in losses)
    peak = -1e18
    mdd = 0.0
    for _, eq in curve:
        peak = max(peak, eq)
        if peak > 0:
            mdd = max(mdd, (peak - eq) / peak * 100)

    fin = broker.balance
    top_by_pnl = {}
    for t in trades:
        top_by_pnl.setdefault(t['sym'], []).append(t['pnl'])
    top_sorted = sorted(top_by_pnl.items(), key=lambda kv: sum(kv[1]), reverse=True)

    bankrupt_day = None
    if bankrupt_at is not None and curve:
        bankrupt_day = round((bankrupt_at - curve[0][0]) / 86_400_000, 1)

    return {
        'mode': 'CROSS_MARGIN',
        'params': {
            'balance': balance, 'days': days, 'fee_pct': broker.fee * 100,
            'n_symbols': n_symbols,
        },
        'stats': stats,
        'bankrupt_at': bankrupt_at,
        'bankrupt_day': bankrupt_day,
        'trades': trades,
        'curve': [[t, b] for t, b in curve],
        'summary': {
            'final': round(fin, 2),
            'pnl_pct': round((fin / balance - 1) * 100, 1),
            'total': len(trades),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': round(pct(len(wins), len(trades)), 1),
            'liqs': len(liqs),
            'liq_pct': round(pct(len(liqs), len(trades)), 1),
            'mean_pnl': round(st_.mean(t['pnl'] for t in trades), 3) if trades else 0,
            'median_pnl': round(st_.median(t['pnl'] for t in trades), 3) if trades else 0,
            'best_pnl': round(max(t['pnl'] for t in trades), 2) if trades else 0,
            'worst_pnl': round(min(t['pnl'] for t in trades), 2) if trades else 0,
            'profit_factor': round(pf_g / pf_l, 2) if pf_l > 0 else (999.99 if pf_g > 0 else 0),
            'total_fees': round(broker.gross_fee_paid, 2),
            'max_dd': round(mdd, 1),
            'survived': fin >= balance * 0.35 and bankrupt_at is None,
            'top_symbols': [{'sym': s, 'pnl': round(sum(v), 2), 'n': len(v)}
                            for s, v in top_sorted[:10]],
            'bottom_symbols': [{'sym': s, 'pnl': round(sum(v), 2), 'n': len(v)}
                               for s, v in top_sorted[-5:]],
        },
    }


def pct(a, b):
    return 100 * a / b if b else 0


GS_MIN_CONF = 0.4


# =============================================================== CLI

async def _cli_main():
    ap = argparse.ArgumentParser(description='Бэктест knife (CROSS MARGIN)')
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--balance', type=float, default=30.0)
    ap.add_argument('--fee-pct', type=float, default=settings.taker_fee_pct)
    ap.add_argument('--min-turnover', type=float, default=5.0)
    ap.add_argument('--min-leverage', type=int, default=settings.min_max_leverage)
    ap.add_argument('--max-symbols', type=int, default=0)
    ap.add_argument('--out', default='backtest_results.json')
    args = ap.parse_args()

    end_ms = int(time.time() * 1000)
    end_ms -= end_ms % _MS['15']
    start_ms = end_ms - args.days * 86_400_000
    warmup_ms = 40 * 86_400_000

    async with aiohttp.ClientSession() as s:
        rest = BybitREST(s)
        print('[1/4] Фильтрую символы как живой монитор...')
        instr = await rest.get_instruments()
        lev_map = await rest.get_max_leverage_map()
        tickers = await rest.get_tickers()
        turn = {t['symbol']: float(t.get('turnover24h') or 0) for t in tickers}
        symbols = []
        for it in instr:
            sym = it['symbol']
            ml = lev_map.get(sym)
            if ml is None or ml < args.min_leverage:
                continue
            if turn.get(sym, 0) / 1e6 < args.min_turnover:
                continue
            symbols.append((sym, ml))
        symbols.sort()
        if args.max_symbols:
            symbols = symbols[:args.max_symbols]
        print(f'      символов в работе: {len(symbols)}')

        print(f'[2/4] Качаю историю {args.days + 40} дней (15m/4h/1d)...')
        sem = asyncio.Semaphore(5)

        async def load(sym):
            async with sem:
                try:
                    kl15 = await fetch_klines(rest, sym, '15', start_ms - 5 * 86_400_000, end_ms)
                    kl4 = await fetch_klines(rest, sym, '240', start_ms - warmup_ms, end_ms)
                    kld = await fetch_klines(rest, sym, 'D', start_ms - warmup_ms, end_ms)
                    if len(kl15) < 200 or len(kl4) < 60 or len(kld) < 30:
                        return sym, None
                    return sym, dict(kl15=kl15, kl4=kl4, kld=kld)
                except Exception:
                    return sym, None

        res = await asyncio.gather(*(load(x) for x, _ in symbols))
        lev_dict = dict(symbols)
        data = {}
        for sym, d in res:
            if d:
                d = {**d, 'lev': min(float(lev_dict[sym]), 125.0)}
                data[sym] = d
        print(f'      загружено: {len(data)}')

        risk_limits = await fetch_risk_limits(rest, set(data.keys()))
        threshold_cache = {sym: symbol_threshold(d['kl4']) for sym, d in data.items()}

    gs = dict(grid_step_pct=5.0, grid_step_multiplier=2.5,
              grid_size_multiplier=1.0, max_steps=10, position_size_entry=6.0,
              position_size_grid=6.0, initial_exit_pct=15.0,
              exit_step_multiplier=1.6, exit_contract_pct=50.0)
    broker = CrossBroker(args.balance, args.fee_pct)
    stats = dict(entries=0, liqs=0, tps=0, skip_no_margin=0, skip_lowvol=0,
                 skip_not_spike=0, skip_lowconf=0)

    print('[3/4] Симулирую (cross margin)...')
    t0 = time.time()
    trades, curve, bankrupt_at = simulate(data, gs, broker, start_ms, end_ms,
                                          stats, risk_limits, threshold_cache)
    print(f'      готово за {time.time()-t0:.1f} c')

    _cli_report(args, broker, trades, curve, data, stats, bankrupt_at)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump({'mode': 'CROSS_MARGIN', 'params': vars(args), 'stats': stats,
                   'bankrupt_at': bankrupt_at,
                   'trades': trades,
                   'curve': [[t, b] for t, b in curve]},
                  f, ensure_ascii=False)
    print(f'\nДетальные результаты -> {os.path.abspath(args.out)}')


def _cli_report(args, broker, trades, curve, data, stats, bankrupt_at):
    days = args.days
    fin = broker.balance
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    liqs = [t for t in trades if t['liq']]
    pf_g = sum(t['pnl'] for t in wins)
    pf_l = -sum(t['pnl'] for t in losses)
    peak = -1e18
    mdd = 0.0
    for _, eq in curve:
        peak = max(peak, eq)
        if peak > 0:
            mdd = max(mdd, (peak - eq) / peak * 100)

    print('\n' + '=' * 72)
    print(f'БЭКТЕСТ ножевого бота (CROSS MARGIN) | {days} дн. | старт {args.balance:.2f} USDT | '
          f'taker {args.fee_pct}%')
    print('=' * 72)
    print(f'Символов в universe: {len(data)}')
    print(f'Сделок закрыто:       {len(trades)}   (входов всего: {stats["entries"]})')
    print(f'  прибыльных:  {len(wins)} ({pct(len(wins), len(trades))}%)')
    print(f'  убыточных:   {len(losses)} ({pct(len(losses), len(trades))}%)')
    print(f'  ликвидаций:  {len(liqs)} ({pct(len(liqs), len(trades))}%)')
    if trades:
        print(f'Средний PnL сделки:  {st_.mean(t["pnl"] for t in trades):+.3f} USDT')
        print(f'Медианный PnL:       {st_.median(t["pnl"] for t in trades):+.3f} USDT')
        print(f'Лучшая / худшая:     {max(t["pnl"] for t in trades):+.2f} / '
              f'{min(t["pnl"] for t in trades):+.2f} USDT')
    print(f'Profit factor:       {(pf_g / pf_l) if pf_l > 0 else float("inf"):.2f}')
    print(f'Комиссий уплачено:   {broker.gross_fee_paid:.2f} USDT')
    print(f'Макс. просадка:      {mdd:.1f}%')
    print('-' * 72)
    if bankrupt_at is not None:
        t = time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(bankrupt_at / 1000))
        n_days = (bankrupt_at - (curve[0][0] if curve else 0)) / 86_400_000
        print(f'ПОЛНОЕ ОБНУЛЕНИЕ Баланса (портфельная ликвидация): {t}')
        print(f'  произошло на {n_days:.1f}-й день теста')
    print(f'ИТОГ ЧЕРЕЗ {days} ДНЕЙ:  {fin:.2f} USDT  '
          f'({(fin / args.balance - 1) * 100:+.1f}%)')
    alive = fin >= args.balance * 0.35 and bankrupt_at is None
    print(f'Выживание: {"ДА" if alive else "НЕТ — депозит уничтожен ликвидацией портфеля"}')
    if trades:
        by_sym = {}
        for t in trades:
            by_sym.setdefault(t['sym'], []).append(t['pnl'])
        top = sorted(by_sym.items(), key=lambda kv: sum(kv[1]), reverse=True)
        print('\nТоп+/топ- символов по суммарному PnL:')
        for s, v in top[:5]:
            print(f'  +{s:<16} {sum(v):+8.2f} USDT  ({len(v)} сделок)')
        for s, v in top[-5:]:
            print(f'  -{s:<16} {sum(v):+8.2f} USDT  ({len(v)} сделок)')
    print('\nСтатистика отказов сигнала: ' + ', '.join(f'{k}={v}' for k, v in stats.items()))


if __name__ == '__main__':
    asyncio.run(_cli_main())