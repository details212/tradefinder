#!/usr/bin/env python3
"""
calculate_studies.py
--------------------
Walk-forward calculation of every technical study used by the candlestick
chart (SMA 50/150/200, EMA 10/20, RSI 14, Heatmap Volume, Donchian 252,
ATR 10, ATR squeeze, pivot highs, VCP detector) for one symbol stored in
daily_ohlcv.

Results are written to `daily_studies`, keyed on (symbol, bar_date) so it
can be INNER JOIN-ed with daily_ohlcv at any time.

All indicator logic is a faithful Python port of the JavaScript in
templates/candlestick.html — same parameters, same walk-forward approach,
zero lookahead bias.

Usage
-----
    python calculate_studies.py --symbol AAPL
    python calculate_studies.py --symbol AAPL --replace   # delete then reinsert
"""

import argparse
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector

# ── DB config ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "127.0.0.1",
    "user":     "remote",
    "password": "Chamba4347!",
    "database": "leo",
}

# ── Parameters (must stay in sync with candlestick.html) ──────────────────────
RSI_PERIOD      = 14
SMA_PERIODS     = [50, 150, 200]
EMA_PERIODS     = [10, 20]
HV_MA_LEN       = 5
HV_STD_LEN      = 5
DONCHIAN_PERIOD = 252
ATR_PERIOD      = 10
ATR_SQUEEZE_LB  = 20   # lookback bars for new ATR low detection
PIVOT_LB        = 5    # bars each side for swing-high confirmation

# VCP
VCP_LB        = 5
VCP_MIN_LEGS  = 2
VCP_MAX_LEGS  = 5
VCP_MAX_FIRST = 0.35
VCP_MIN_LAST  = 0.01
VCP_MAX_LAST  = 0.08
VCP_VW        = 1.5   # volume contraction weight

# Absolute Strength (AS) Rating — trading-day lookback windows
AS_PERIODS = {
    '1m':  21,   # ~1 month
    '3m':  63,   # ~3 months
    '6m':  126,  # ~6 months
    '12m': 252,  # ~12 months
}

# Relative Market Volatility (RMV) lookback windows
RMV_PERIODS = [5, 15, 50]

# Relative Strength (RS) Line vs. benchmark
RS_BENCHMARK    = 'SPY'
RS_EMA_PERIOD   = 21    # fast EMA of the ratio — short-term trend
RS_SMA_PERIOD   = 50    # slow SMA of the ratio — long-term trend
RS_NEW_HIGH_LB  = 252   # lookback for RS-line new-high flag

# Heatmap z-score thresholds (match JS T_XHIGH … T_NORM)
HV_T_XHIGH =  1.75
HV_T_HIGH  =  1.50
HV_T_MED   =  1.00
HV_T_NORM  = -1.00

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS daily_studies (
    symbol            VARCHAR(20)    NOT NULL,
    bar_date          DATE           NOT NULL,
    -- Moving averages
    sma_50            DECIMAL(14,4)  DEFAULT NULL,
    sma_150           DECIMAL(14,4)  DEFAULT NULL,
    sma_200           DECIMAL(14,4)  DEFAULT NULL,
    ema_10            DECIMAL(14,4)  DEFAULT NULL,
    ema_20            DECIMAL(14,4)  DEFAULT NULL,
    -- Momentum
    rsi_14            DECIMAL(7,4)   DEFAULT NULL,
    rsi_cross_50      TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 on the bar RSI crosses above 50
    rsi_cross_down_50 TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 on the bar RSI crosses below 50
    -- Heatmap Volume
    vol_ma            DECIMAL(20,2)  DEFAULT NULL,
    vol_label         VARCHAR(10)    DEFAULT NULL,   -- XHigh/High/Med/Norm/Low
    -- Donchian 252
    dc_upper          DECIMAL(14,4)  DEFAULT NULL,
    dc_lower          DECIMAL(14,4)  DEFAULT NULL,
    -- ATR(10) compression
    atr_10            DECIMAL(14,4)  DEFAULT NULL,
    atr_squeeze       TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 when ATR is at a 20-bar low
    -- Swing-high pivot level (most recent confirmed swing high as of each bar)
    pivot_high        DECIMAL(14,4)  DEFAULT NULL,
    -- ATR streak analysis
    atr_declining_bars  SMALLINT     DEFAULT NULL,   -- current consecutive declining streak (0 if rising)
    atr_streak_len      SMALLINT     DEFAULT NULL,   -- length of last completed declining streak
    atr_streak_ago      SMALLINT     DEFAULT NULL,   -- bars since that streak ended (NULL if currently declining)
    -- VCP (Volatility Contraction Pattern) walk-forward detector
    vcp_score         DECIMAL(7,4)   DEFAULT NULL,
    vcp_pivot         DECIMAL(14,4)  DEFAULT NULL,
    vcp_vol_score     DECIMAL(7,4)   DEFAULT NULL,
    vcp_depth_score   DECIMAL(7,4)   DEFAULT NULL,
    vcp_num_legs      TINYINT        DEFAULT NULL,
    vcp_is_signal     TINYINT(1)     NOT NULL DEFAULT 0,
    -- Absolute Strength (AS) Rating
    as_raw_1m         DECIMAL(14,4)  DEFAULT NULL,   -- raw % change over ~1 month  (21 bars)
    as_raw_3m         DECIMAL(14,4)  DEFAULT NULL,   -- raw % change over ~3 months (63 bars)
    as_raw_6m         DECIMAL(14,4)  DEFAULT NULL,   -- raw % change over ~6 months (126 bars)
    as_raw_12m        DECIMAL(14,4)  DEFAULT NULL,   -- raw % change over ~12 months (252 bars)
    as_1m             TINYINT UNSIGNED DEFAULT NULL, -- cross-sectional percentile rank 1-99
    as_3m             TINYINT UNSIGNED DEFAULT NULL,
    as_6m             TINYINT UNSIGNED DEFAULT NULL,
    as_12m            TINYINT UNSIGNED DEFAULT NULL,
    -- Relative Market Volatility (RMV)
    rmv_5             DECIMAL(10,4)  DEFAULT NULL,   -- (current range / avg range 5 bars)  × 100
    rmv_15            DECIMAL(10,4)  DEFAULT NULL,   -- (current range / avg range 15 bars) × 100  [default]
    rmv_50            DECIMAL(10,4)  DEFAULT NULL,   -- (current range / avg range 50 bars) × 100
    -- Relative Strength (RS) Line vs. SPY
    rs_line           DECIMAL(14,6)  DEFAULT NULL,   -- stock_close / SPY_close
    rs_ema_21         DECIMAL(14,6)  DEFAULT NULL,   -- 21-bar EMA of the ratio (fast trend)
    rs_sma_50         DECIMAL(14,6)  DEFAULT NULL,   -- 50-bar SMA of the ratio (slow trend)
    rs_new_high            TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 when RS line at new 252-bar high
    rs_cross_above_sma50   TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 on the bar RS line crosses above rs_sma_50
    rs_cross_below_sma50   TINYINT(1)     NOT NULL DEFAULT 0,  -- 1 on the bar RS line crosses below rs_sma_50
    PRIMARY KEY (symbol, bar_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ── Indicator functions ────────────────────────────────────────────────────────

def calc_sma(closes: list, period: int) -> list:
    """Simple moving average.  Output aligned to input (None until warm)."""
    n   = len(closes)
    out = [None] * n
    s   = sum(closes[:period - 1])
    for i in range(period - 1, n):
        s      += closes[i]
        out[i]  = s / period
        s      -= closes[i - period + 1]
    return out


def calc_ema(closes: list, period: int) -> list:
    """
    EMA seeded with the SMA of the first `period` bars (matches JS).
    Output aligned to input; first value at index period-1.
    """
    n   = len(closes)
    out = [None] * n
    if n < period:
        return out
    k   = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    out[period - 1] = ema
    for i in range(period, n):
        ema    = (closes[i] - ema) * k + ema
        out[i] = ema
    return out


def calc_rsi(closes: list, period: int = 14) -> list:
    """
    Wilder's smoothed RSI.
    Seed: simple average of first `period` changes.
    Output aligned to input; first value at index `period`.
    """
    n   = len(closes)
    out = [None] * n
    if n < period + 1:
        return out

    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        gains.append(d  if d > 0 else 0.0)
        losses.append(-d if d < 0 else 0.0)

    avg_g = sum(gains[:period])  / period
    avg_l = sum(losses[:period]) / period

    def _rsi(ag, al):
        if al == 0: return 100.0
        if ag == 0: return 0.0
        return 100.0 - 100.0 / (1.0 + ag / al)

    out[period] = round(_rsi(avg_g, avg_l), 2)
    for i in range(period, len(gains)):
        avg_g  = (avg_g  * (period - 1) + gains[i])  / period
        avg_l  = (avg_l  * (period - 1) + losses[i]) / period
        out[i + 1] = round(_rsi(avg_g, avg_l), 2)

    return out


def calc_heatmap_volume(volumes: list, ma_len: int = 5, std_len: int = 5):
    """
    Heatmap Volume — port of the Pine Script / JS implementation.

    Uses a growing window at the start (same as JS Math.max(0, i-len+1)).
    Returns (vol_ma, vol_label) lists aligned to input.
    vol_label ∈ {'XHigh', 'High', 'Med', 'Norm', 'Low'}
    """
    n             = len(volumes)
    vol_ma_out    = [None] * n
    vol_label_out = [None] * n

    for i in range(n):
        # MA window (matches JS: mStart = max(0, i - maLen + 1))
        m_start  = max(0, i - ma_len  + 1)
        m_win    = volumes[m_start : i + 1]
        mean     = sum(m_win) / len(m_win)

        # Std window (matches JS: sStart = max(0, i - stdLen + 1))
        s_start  = max(0, i - std_len + 1)
        s_win    = volumes[s_start : i + 1]
        s_mean   = sum(s_win) / len(s_win)
        variance = sum((v - s_mean) ** 2 for v in s_win) / len(s_win)
        std      = math.sqrt(variance) if variance > 0 else 0.0

        vol_ma_out[i] = round(mean, 2)

        stdbar = (volumes[i] - mean) / std if std > 0 else 0.0

        if   stdbar > HV_T_XHIGH: lbl = 'XHigh'
        elif stdbar > HV_T_HIGH:  lbl = 'High'
        elif stdbar > HV_T_MED:   lbl = 'Med'
        elif stdbar > HV_T_NORM:  lbl = 'Norm'
        else:                     lbl = 'Low'

        vol_label_out[i] = lbl

    return vol_ma_out, vol_label_out


def calc_donchian(highs: list, lows: list, period: int = 252):
    """
    Donchian channel.  Output aligned to input; first value at index period-1.
    """
    n     = len(highs)
    upper = [None] * n
    lower = [None] * n
    for i in range(period - 1, n):
        upper[i] = max(highs[i - period + 1 : i + 1])
        lower[i] = min(lows [i - period + 1 : i + 1])
    return upper, lower


def calc_atr(highs: list, lows: list, closes: list, period: int = 10) -> list:
    """
    Wilder's smoothed ATR.  Port of calcATR() in candlestick.html.
    Output aligned to input; first value at index `period`.
    """
    n   = len(closes)
    out = [None] * n
    if n < period + 1:
        return out

    trs = []
    for i in range(1, n):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i - 1]),
            abs(lows[i]   - closes[i - 1]),
        )
        trs.append(tr)

    atr = sum(trs[:period]) / period
    out[period] = round(atr, 4)
    for i in range(period, len(trs)):
        atr    = (atr * (period - 1) + trs[i]) / period
        out[i + 1] = round(atr, 4)

    return out


def calc_atr_squeeze(atr_values: list, lookback: int = 20) -> list:
    """
    Returns a 0/1 list.  1 where the ATR value is at a new `lookback`-bar low.
    Port of calcATRSqueeze() in candlestick.html.
    """
    n   = len(atr_values)
    out = [0] * n
    for i in range(lookback - 1, n):
        cur = atr_values[i]
        if cur is None:
            continue
        is_low = all(
            atr_values[j] is None or atr_values[j] > cur
            for j in range(i - lookback + 1, i)
        )
        if is_low:
            out[i] = 1
    return out


def calc_pivot_highs(highs: list, lb: int = 5) -> list:
    """
    Walk-forward swing-high detector.  Port of calcPivotHighs() in candlestick.html.

    A bar at index `pivot_idx` is a confirmed swing high once we have seen
    `lb` bars to its right (i.e. at bar `pivot_idx + lb`).  The most recent
    confirmed pivot level is then carried forward to every subsequent bar.

    Returns the most recent swing-high price as of each bar (None until the
    first swing high is confirmed).
    """
    n           = len(highs)
    out         = [None] * n
    last_pivot  = None

    for i in range(n):
        # The bar that becomes confirmable when we reach bar i is (i - lb)
        pivot_idx = i - lb
        if pivot_idx >= lb:
            h  = highs[pivot_idx]
            ok = True
            for j in range(pivot_idx - lb, pivot_idx + lb + 1):
                if j != pivot_idx and highs[j] >= h:
                    ok = False
                    break
            if ok:
                last_pivot = h
        out[i] = last_pivot

    return out


def calc_atr_streaks(atr_values: list):
    """
    Walk-forward streak analysis on ATR values.

    Returns three lists aligned to input:
      atr_declining_bars  int   — current consecutive declining streak length (0 if ATR is rising)
      atr_streak_len      int   — length of the most recently *completed* declining streak (None if no prior streak)
      atr_streak_ago      int   — bars since that completed streak ended (None if currently declining or no prior streak)

    Display logic:
      atr_declining_bars > 0  → stock is currently compressing  ("↓ N bars")
      atr_declining_bars = 0  → compression ended; show "M bars ago · L bars long"
    """
    n = len(atr_values)
    atr_declining_bars = [0]   * n
    atr_streak_len     = [None] * n
    atr_streak_ago     = [None] * n

    current_streak        = 0
    last_completed_len    = None
    last_completed_end    = None   # index of the last bar of the most recent completed streak

    for i in range(1, n):
        prev = atr_values[i - 1]
        cur  = atr_values[i]

        if cur is None or prev is None:
            if current_streak > 0:
                last_completed_len = current_streak
                last_completed_end = i - 1
            current_streak = 0
        elif cur < prev:
            current_streak += 1
        else:
            if current_streak > 0:
                last_completed_len = current_streak
                last_completed_end = i - 1
            current_streak = 0

        atr_declining_bars[i] = current_streak

        if current_streak == 0:
            atr_streak_len[i] = last_completed_len
            atr_streak_ago[i] = (i - last_completed_end) if last_completed_end is not None else None
        # else: None (currently in an active streak — use atr_declining_bars instead)

    return atr_declining_bars, atr_streak_len, atr_streak_ago


def calc_vcp(highs: list, lows: list, volumes: list):
    """
    Walk-forward VCP detector — faithful port of runVCPScan() in candlestick.html.

    Returns a list (same length as input) of dicts or None.  Each non-None entry:
        vcp_score       float   combined contraction quality 0-100
        vcp_pivot       float   highest high of last 2 legs  (breakout level)
        vcp_vol_score   float
        vcp_depth_score float
        vcp_num_legs    int
        vcp_is_signal   int     1 if this bar is a gate-qualified VCP signal
    """
    LB        = VCP_LB
    MIN_LEGS  = VCP_MIN_LEGS
    MAX_LEGS  = VCP_MAX_LEGS
    MAX_FIRST = VCP_MAX_FIRST
    MIN_LAST  = VCP_MIN_LAST
    MAX_LAST  = VCP_MAX_LAST
    VW        = VCP_VW

    n      = len(highs)
    result = [None] * n

    # ── swing-point detectors (exact port of JS isSwingHigh / isSwingLow) ─────
    def is_swing_high(i: int) -> bool:
        if i < LB or i + LB >= n:
            return False
        c = highs[i]
        for j in range(i - LB, i):
            if c <= highs[j]: return False        # strictly greater than left bars
        for j in range(i + 1, i + LB + 1):
            if c < highs[j]:  return False        # >= right bars
        return True

    def is_swing_low(i: int) -> bool:
        if i < LB or i + LB >= n:
            return False
        c = lows[i]
        for j in range(i - LB, i):
            if c >= lows[j]: return False         # strictly less than left bars
        for j in range(i + 1, i + LB + 1):
            if c > lows[j]:  return False         # <= right bars
        return True

    def build_leg(s: int, e: int):
        """High = highs[s], Low = lows[e] (matches JS: l = lows[e])."""
        if e <= s:
            return None
        h     = highs[s]
        lo    = lows[e]
        depth = (h - lo) / h if h > 0 else 0.0
        if depth < 0.005:
            return None
        avg_vol = sum(volumes[s : e + 1]) / (e - s + 1)
        return {'s': s, 'e': e, 'high': h, 'low': lo,
                'depth': depth, 'avgVol': avg_vol}

    def contraction_score(vals: list, wr: float = 1.0) -> float:
        """
        Weighted contraction score (0-100).
        Later pairs carry more weight when wr > 1 (used for volume with VW=1.5).
        Partial credit (×0.5) for equal consecutive values.
        Matches JS contractionScore() exactly.
        """
        np_ = len(vals) - 1
        if np_ < 1:
            return 0.0
        total_w = 0.0
        score   = 0.0
        for i in range(np_):
            w        = 1.0 if np_ == 1 else 1.0 + (wr - 1.0) * i / (np_ - 1)
            total_w += w
            if   vals[i + 1] <  vals[i]: score += w
            elif vals[i + 1] == vals[i]: score += w * 0.5
        return (score / total_w * 100) if total_w > 0 else 0.0

    def eval_vcp(legs: list):
        """Gate-qualified VCP signal (ds >= 50, vs >= 50 + depth gates)."""
        if len(legs) < MIN_LEGS:
            return None
        ls = legs[-MAX_LEGS:]
        if ls[0]['depth'] > MAX_FIRST:
            return None
        last = ls[-1]
        if last['depth'] < MIN_LAST or last['depth'] > MAX_LAST:
            return None
        ds = contraction_score([l['depth']  for l in ls])
        if ds < 50:
            return None
        vs = contraction_score([l['avgVol'] for l in ls], VW)
        if vs < 50:
            return None
        combined = ds * 0.4 + vs * 0.6
        pivot    = max(l['high'] for l in ls[-2:])
        return {'depthScore': ds, 'volScore': vs, 'combined': combined,
                'pivot': pivot, 'numLegs': len(ls)}

    def eval_vcp_raw(legs: list):
        """Raw scorer — no hard gates, same source as the perBar chart."""
        if len(legs) < MIN_LEGS:
            return None
        ls       = legs[-MAX_LEGS:]
        ds       = contraction_score([l['depth']  for l in ls])
        vs       = contraction_score([l['avgVol'] for l in ls], VW)
        combined = round(ds * 0.4 + vs * 0.6, 1)
        pivot    = max(l['high'] for l in ls[-2:])
        return {'depthScore': ds, 'volScore': vs, 'combined': combined,
                'pivot': pivot, 'numLegs': len(ls)}

    # ── walk-forward scan ──────────────────────────────────────────────────────
    completed_legs: list  = []
    in_down:        bool  = False
    leg_start:      int   = -1
    raw_at_idx:     dict  = {}
    signal_set:     set   = set()

    for idx in range(LB * 2, n):
        ci = idx - LB
        if is_swing_high(ci):
            in_down   = True
            leg_start = ci
        if is_swing_low(ci) and in_down and leg_start >= 0:
            leg = build_leg(leg_start, ci)
            if leg:
                completed_legs.append(leg)
                if len(completed_legs) > MAX_LEGS:
                    completed_legs.pop(0)
            in_down = False
            if eval_vcp(completed_legs):
                signal_set.add(ci)
            raw = eval_vcp_raw(completed_legs)
            if raw:
                raw_at_idx[ci] = raw

    # Carry last raw score forward bar-by-bar (same as JS perBar build loop)
    cur_raw = None
    for i in range(n):
        if i in raw_at_idx:
            cur_raw = raw_at_idx[i]
        if cur_raw is not None:
            result[i] = {
                'vcp_score':       cur_raw['combined'],
                'vcp_pivot':       cur_raw['pivot'],
                'vcp_vol_score':   cur_raw['volScore'],
                'vcp_depth_score': cur_raw['depthScore'],
                'vcp_num_legs':    cur_raw['numLegs'],
                'vcp_is_signal':   1 if i in signal_set else 0,
            }

    return result


def calc_as_raw(closes: list) -> dict:
    """
    Absolute Strength raw scores — percentage price change over fixed trading-day
    lookback windows (1M≈21, 3M≈63, 6M≈126, 12M≈252 bars).

    Returns a dict keyed by period name, each value is a list aligned to the
    input closes list (None until the lookback window is fully available).

    The cross-sectional percentile ranking (the 1–99 AS Rating) is computed
    separately by run_as_ranking() after all symbols have been processed.
    """
    n      = len(closes)
    result = {k: [None] * n for k in AS_PERIODS}
    for key, lb in AS_PERIODS.items():
        arr = result[key]
        for i in range(lb, n):
            prior = closes[i - lb]
            if prior and prior != 0.0:
                arr[i] = round((closes[i] - prior) / prior * 100, 4)
    return result


def calc_rmv(highs: list, lows: list, closes: list,
             periods: list = None) -> dict:
    """
    Relative Market Volatility (RMV) — True Range percentile rank.

    RMV(N) = (bars in window with TR < today's TR) / (window size) × 100

    For each bar, counts what percentage of the N-bar window (including today)
    had a smaller True Range.  This is a pure percentile rank:

      RMV = 0    — today is the tightest bar in the window (maximum coiling)
      RMV = 100  — today is the widest bar in the window (maximum expansion)
      RMV < 15   — high-conviction tight setup
      RMV < 25   — actionable tightness signal

    Being a percentile rank (not a ratio) it reaches true zero during genuine
    consolidation, is self-normalising across price levels, and is always
    naturally bounded 0–100 with no capping required.

    True Range (max of H-L, |H-prevClose|, |L-prevClose|) handles gap days
    that simple High-Low misses.

    First value available at bar index N (needs N prior bars in the window).
    """
    if periods is None:
        periods = RMV_PERIODS

    n = len(closes)

    # True Range — first bar has no prior close so fall back to H-L
    tr = [highs[0] - lows[0]] + [
        max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        for i in range(1, n)
    ]

    result = {p: [None] * n for p in periods}
    for p in periods:
        arr = result[p]
        for i in range(p, n):                    # needs p prior bars + today
            window = tr[i - p : i + 1]           # p+1 values including today
            today  = tr[i]
            pct    = sum(1 for v in window if v < today) / len(window) * 100
            arr[i] = round(pct, 4)

    return result


def calc_rs_line(closes: list, dates: list, benchmark: dict):
    """
    Relative Strength Line vs. a benchmark (default: SPY).

    rs_line[i]    = stock_close[i] / benchmark_close[date[i]]
    rs_ema_21[i]  = 21-bar EMA of the ratio (fast trend — crosses SMA signal direction changes)
    rs_sma_50[i]  = 50-bar SMA of the ratio (slow trend — area fill reference)
    rs_new_high[i]= 1 when rs_line[i] is at a new 252-bar high

    Chart usage:
      • Fill area between rs_line and rs_sma_50 blue when rs_line > rs_sma_50
        (outperforming), gray when below (underperforming).
      • rs_ema_21 crossing above rs_sma_50 = bullish RS momentum shift.
      • rs_new_high triangle markers flag leading-strength breakouts.

    benchmark: {bar_date: float} dict — pre-fetched via fetch_benchmark().
    All output lists are aligned to the input.
    """
    n  = len(closes)
    rs = [None] * n

    for i in range(n):
        bm = benchmark.get(dates[i])
        if bm and bm > 0:
            rs[i] = round(closes[i] / bm, 6)

    # 21-bar EMA of the ratio (seeded with SMA of first 21 valid values)
    rs_ema = [None] * n
    k      = 2.0 / (RS_EMA_PERIOD + 1)
    seed_vals = []
    seed_idx  = None
    for i in range(n):
        if rs[i] is None:
            seed_vals = []           # reset if a gap appears before seeding
            continue
        seed_vals.append(rs[i])
        if len(seed_vals) == RS_EMA_PERIOD:
            ema      = sum(seed_vals) / RS_EMA_PERIOD
            rs_ema[i] = round(ema, 6)
            seed_idx  = i
            break
    if seed_idx is not None:
        ema = rs_ema[seed_idx]
        for i in range(seed_idx + 1, n):
            if rs[i] is None:
                continue
            ema       = (rs[i] - ema) * k + ema
            rs_ema[i] = round(ema, 6)

    # 50-bar SMA of the ratio; None if any bar in window is missing
    rs_sma = [None] * n
    for i in range(RS_SMA_PERIOD - 1, n):
        window = rs[i - RS_SMA_PERIOD + 1 : i + 1]
        if all(v is not None for v in window):
            rs_sma[i] = round(sum(window) / RS_SMA_PERIOD, 6)

    # New 252-bar high on the RS line
    rs_hi = [0] * n
    for i in range(RS_NEW_HIGH_LB - 1, n):
        cur = rs[i]
        if cur is None:
            continue
        prev = rs[i - RS_NEW_HIGH_LB + 1 : i]
        if all(v is None or cur > v for v in prev):
            rs_hi[i] = 1

    return rs, rs_ema, rs_sma, rs_hi


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute(DDL)
    # Add any columns introduced after the initial table creation
    migrations = [
        "ALTER TABLE daily_studies ADD COLUMN rsi_cross_50       TINYINT(1)    NOT NULL DEFAULT 0    AFTER rsi_14",
        "ALTER TABLE daily_studies ADD COLUMN rsi_cross_down_50  TINYINT(1)    NOT NULL DEFAULT 0    AFTER rsi_cross_50",
        "ALTER TABLE daily_studies ADD COLUMN atr_10             DECIMAL(14,4) DEFAULT NULL        AFTER dc_lower",
        "ALTER TABLE daily_studies ADD COLUMN atr_squeeze        TINYINT(1)    NOT NULL DEFAULT 0 AFTER atr_10",
        "ALTER TABLE daily_studies ADD COLUMN pivot_high         DECIMAL(14,4) DEFAULT NULL        AFTER atr_squeeze",
        "ALTER TABLE daily_studies ADD COLUMN atr_declining_bars SMALLINT      DEFAULT NULL        AFTER pivot_high",
        "ALTER TABLE daily_studies ADD COLUMN atr_streak_len     SMALLINT      DEFAULT NULL        AFTER atr_declining_bars",
        "ALTER TABLE daily_studies ADD COLUMN atr_streak_ago     SMALLINT      DEFAULT NULL        AFTER atr_streak_len",
        "ALTER TABLE daily_studies ADD COLUMN as_raw_1m  DECIMAL(14,4)    DEFAULT NULL AFTER vcp_is_signal",
        "ALTER TABLE daily_studies ADD COLUMN as_raw_3m  DECIMAL(14,4)    DEFAULT NULL AFTER as_raw_1m",
        "ALTER TABLE daily_studies ADD COLUMN as_raw_6m  DECIMAL(14,4)    DEFAULT NULL AFTER as_raw_3m",
        "ALTER TABLE daily_studies ADD COLUMN as_raw_12m DECIMAL(14,4)    DEFAULT NULL AFTER as_raw_6m",
        "ALTER TABLE daily_studies ADD COLUMN as_1m      TINYINT UNSIGNED DEFAULT NULL AFTER as_raw_12m",
        "ALTER TABLE daily_studies ADD COLUMN as_3m      TINYINT UNSIGNED DEFAULT NULL AFTER as_1m",
        "ALTER TABLE daily_studies ADD COLUMN as_6m      TINYINT UNSIGNED DEFAULT NULL AFTER as_3m",
        "ALTER TABLE daily_studies ADD COLUMN as_12m     TINYINT UNSIGNED DEFAULT NULL AFTER as_6m",
        # Widen as_raw_* if already created with the narrower DECIMAL(10,4)
        "ALTER TABLE daily_studies MODIFY COLUMN as_raw_1m  DECIMAL(14,4) DEFAULT NULL",
        "ALTER TABLE daily_studies MODIFY COLUMN as_raw_3m  DECIMAL(14,4) DEFAULT NULL",
        "ALTER TABLE daily_studies MODIFY COLUMN as_raw_6m  DECIMAL(14,4) DEFAULT NULL",
        "ALTER TABLE daily_studies MODIFY COLUMN as_raw_12m DECIMAL(14,4) DEFAULT NULL",
        "ALTER TABLE daily_studies ADD COLUMN rmv_5  DECIMAL(10,4) DEFAULT NULL AFTER as_12m",
        "ALTER TABLE daily_studies ADD COLUMN rmv_15 DECIMAL(10,4) DEFAULT NULL AFTER rmv_5",
        "ALTER TABLE daily_studies ADD COLUMN rmv_50 DECIMAL(10,4) DEFAULT NULL AFTER rmv_15",
        "ALTER TABLE daily_studies ADD COLUMN rs_line     DECIMAL(14,6) DEFAULT NULL          AFTER rmv_50",
        "ALTER TABLE daily_studies ADD COLUMN rs_ema_21   DECIMAL(14,6) DEFAULT NULL          AFTER rs_line",
        "ALTER TABLE daily_studies ADD COLUMN rs_sma_50   DECIMAL(14,6) DEFAULT NULL          AFTER rs_ema_21",
        "ALTER TABLE daily_studies ADD COLUMN rs_new_high          TINYINT(1)    NOT NULL DEFAULT 0    AFTER rs_sma_50",
        "ALTER TABLE daily_studies ADD COLUMN rs_cross_above_sma50 TINYINT(1)    NOT NULL DEFAULT 0    AFTER rs_new_high",
        "ALTER TABLE daily_studies ADD COLUMN rs_cross_below_sma50 TINYINT(1)    NOT NULL DEFAULT 0    AFTER rs_cross_above_sma50",
    ]
    for sql in migrations:
        try:
            cur.execute(sql)
            conn.commit()
        except mysql.connector.errors.DatabaseError as e:
            if e.errno == 1060:  # Duplicate column — already exists, skip
                pass
            else:
                raise
    cur.close()


def fetch_ohlcv(conn, symbol: str) -> list:
    """Returns list of dicts: bar_date, open, high, low, close, volume — ordered by date."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT bar_date, open, high, low, close, volume
        FROM   daily_ohlcv
        WHERE  symbol = %s
        ORDER  BY bar_date
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_benchmark(conn, symbol: str = RS_BENCHMARK) -> dict:
    """Returns {bar_date: close} for the benchmark symbol (default SPY)."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT bar_date, close FROM daily_ohlcv WHERE symbol = %s ORDER BY bar_date",
        (symbol,),
    )
    rows = cur.fetchall()
    cur.close()
    return {r['bar_date']: float(r['close']) for r in rows}


def delete_symbol(conn, symbol: str):
    cur = conn.cursor()
    cur.execute("DELETE FROM daily_studies WHERE symbol = %s", (symbol,))
    conn.commit()
    cur.close()


UPSERT_SQL = """
INSERT INTO daily_studies
    (symbol, bar_date,
     sma_50, sma_150, sma_200,
     ema_10, ema_20,
     rsi_14, rsi_cross_50, rsi_cross_down_50,
     vol_ma, vol_label,
     dc_upper, dc_lower,
     atr_10, atr_squeeze, pivot_high,
     atr_declining_bars, atr_streak_len, atr_streak_ago,
     vcp_score, vcp_pivot, vcp_vol_score, vcp_depth_score,
     vcp_num_legs, vcp_is_signal,
     as_raw_1m, as_raw_3m, as_raw_6m, as_raw_12m,
     rmv_5, rmv_15, rmv_50,
     rs_line, rs_ema_21, rs_sma_50, rs_new_high,
     rs_cross_above_sma50, rs_cross_below_sma50)
VALUES
    (%s, %s,
     %s, %s, %s,
     %s, %s,
     %s, %s, %s,
     %s, %s,
     %s, %s,
     %s, %s, %s,
     %s, %s, %s,
     %s, %s, %s, %s,
     %s, %s,
     %s, %s, %s, %s,
     %s, %s, %s,
     %s, %s, %s, %s,
     %s, %s)
ON DUPLICATE KEY UPDATE
    sma_50          = VALUES(sma_50),
    sma_150         = VALUES(sma_150),
    sma_200         = VALUES(sma_200),
    ema_10          = VALUES(ema_10),
    ema_20          = VALUES(ema_20),
    rsi_14          = VALUES(rsi_14),
    rsi_cross_50         = VALUES(rsi_cross_50),
    rsi_cross_down_50    = VALUES(rsi_cross_down_50),
    vol_ma          = VALUES(vol_ma),
    vol_label       = VALUES(vol_label),
    dc_upper        = VALUES(dc_upper),
    dc_lower        = VALUES(dc_lower),
    atr_10              = VALUES(atr_10),
    atr_squeeze         = VALUES(atr_squeeze),
    pivot_high          = VALUES(pivot_high),
    atr_declining_bars  = VALUES(atr_declining_bars),
    atr_streak_len      = VALUES(atr_streak_len),
    atr_streak_ago      = VALUES(atr_streak_ago),
    vcp_score       = VALUES(vcp_score),
    vcp_pivot       = VALUES(vcp_pivot),
    vcp_vol_score   = VALUES(vcp_vol_score),
    vcp_depth_score = VALUES(vcp_depth_score),
    vcp_num_legs    = VALUES(vcp_num_legs),
    vcp_is_signal   = VALUES(vcp_is_signal),
    as_raw_1m       = VALUES(as_raw_1m),
    as_raw_3m       = VALUES(as_raw_3m),
    as_raw_6m       = VALUES(as_raw_6m),
    as_raw_12m      = VALUES(as_raw_12m),
    rmv_5           = VALUES(rmv_5),
    rmv_15          = VALUES(rmv_15),
    rmv_50          = VALUES(rmv_50),
    rs_line         = VALUES(rs_line),
    rs_ema_21       = VALUES(rs_ema_21),
    rs_sma_50       = VALUES(rs_sma_50),
    rs_new_high          = VALUES(rs_new_high),
    rs_cross_above_sma50 = VALUES(rs_cross_above_sma50),
    rs_cross_below_sma50 = VALUES(rs_cross_below_sma50)
"""


def _r4(v):
    """Round to 4 dp, or None."""
    return round(float(v), 4) if v is not None else None


def _r2(v):
    return round(float(v), 2) if v is not None else None


# ── Main ───────────────────────────────────────────────────────────────────────

def run(symbol: str, replace: bool, silent: bool = False,
        benchmark_prices: dict = None) -> dict:
    """
    Calculate and persist all studies for one symbol.

    benchmark_prices: pre-fetched {bar_date: close} for the RS benchmark (SPY).
                      If None it is fetched from the DB automatically.  In bulk
                      mode, pass the same dict to every worker to avoid
                      re-fetching SPY once per symbol.

    Returns a dict: {'symbol': ..., 'bars': int, 'elapsed': float, 'error': str|None}
    Raises on unexpected errors only when silent=False (single-symbol mode).
    """
    def log(msg):
        if not silent:
            print(msg)

    t0 = time.time()
    log(f"[calculate_studies] symbol={symbol}  replace={replace}")

    try:
        conn = get_conn()
        ensure_table(conn)

        # ── Fetch benchmark prices (SPY) if not pre-supplied ──────────────────
        if benchmark_prices is None:
            benchmark_prices = fetch_benchmark(conn)

        # ── Fetch OHLCV ────────────────────────────────────────────────────────
        rows = fetch_ohlcv(conn, symbol)
        if not rows:
            conn.close()
            msg = f"no rows found in daily_ohlcv for symbol={symbol!r}"
            if not silent:
                print(f"  ERROR: {msg}")
                sys.exit(1)
            return {'symbol': symbol, 'bars': 0, 'elapsed': time.time() - t0, 'error': msg}

        log(f"  Loaded {len(rows):,} bars  ({rows[0]['bar_date']} to {rows[-1]['bar_date']})")

        closes  = [float(r['close'])  for r in rows]
        highs   = [float(r['high'])   for r in rows]
        lows    = [float(r['low'])    for r in rows]
        volumes = [int(r['volume'])   for r in rows]
        dates   = [r['bar_date']      for r in rows]

        # ── Calculate indicators ───────────────────────────────────────────────
        log("  Calculating SMAs ...")
        sma50  = calc_sma(closes, 50)
        sma150 = calc_sma(closes, 150)
        sma200 = calc_sma(closes, 200)

        log("  Calculating EMAs ...")
        ema10  = calc_ema(closes, 10)
        ema20  = calc_ema(closes, 20)

        log("  Calculating RSI 14 ...")
        rsi14       = calc_rsi(closes, 14)
        rsi_cross50 = [
            1 if (rsi14[i] is not None and rsi14[i] >= 50 and
                  rsi14[i - 1] is not None and rsi14[i - 1] < 50)
            else 0
            for i in range(len(rsi14))
        ]
        rsi_cross_down50 = [
            1 if (rsi14[i] is not None and rsi14[i] < 50 and
                  rsi14[i - 1] is not None and rsi14[i - 1] >= 50)
            else 0
            for i in range(len(rsi14))
        ]

        log("  Calculating Heatmap Volume ...")
        vol_ma, vol_lbl = calc_heatmap_volume(volumes, HV_MA_LEN, HV_STD_LEN)

        log("  Calculating Donchian 252 ...")
        dc_upper, dc_lower = calc_donchian(highs, lows, DONCHIAN_PERIOD)

        log("  Calculating ATR 10 ...")
        atr10                              = calc_atr(highs, lows, closes, ATR_PERIOD)
        atr_sq                             = calc_atr_squeeze(atr10, ATR_SQUEEZE_LB)
        atr_decl, atr_slen, atr_sago      = calc_atr_streaks(atr10)

        log("  Calculating pivot highs ...")
        pivot_high = calc_pivot_highs(highs, PIVOT_LB)

        log("  Running VCP walk-forward scan ...")
        vcp = calc_vcp(highs, lows, volumes)

        log("  Calculating Absolute Strength raw scores ...")
        as_raw = calc_as_raw(closes)

        log("  Calculating RMV (5/15/50) ...")
        rmv = calc_rmv(highs, lows, closes)

        log(f"  Calculating RS Line vs {RS_BENCHMARK} ...")
        rs_line, rs_ema21, rs_sma50, rs_new_hi = calc_rs_line(closes, dates, benchmark_prices)
        rs_cross_above_sma50 = [
            1 if (rs_line[i] is not None and rs_sma50[i] is not None and
                  rs_line[i - 1] is not None and rs_sma50[i - 1] is not None and
                  rs_line[i] >= rs_sma50[i] and rs_line[i - 1] < rs_sma50[i - 1])
            else 0
            for i in range(len(rs_line))
        ]
        rs_cross_below_sma50 = [
            1 if (rs_line[i] is not None and rs_sma50[i] is not None and
                  rs_line[i - 1] is not None and rs_sma50[i - 1] is not None and
                  rs_line[i] < rs_sma50[i] and rs_line[i - 1] >= rs_sma50[i - 1])
            else 0
            for i in range(len(rs_line))
        ]

        # ── Build rows ─────────────────────────────────────────────────────────
        n    = len(rows)
        data = []
        for i in range(n):
            v = vcp[i] or {}
            data.append((
                symbol, dates[i],
                _r4(sma50[i]), _r4(sma150[i]), _r4(sma200[i]),
                _r4(ema10[i]), _r4(ema20[i]),
                _r4(rsi14[i]), rsi_cross50[i], rsi_cross_down50[i],
                _r2(vol_ma[i]), vol_lbl[i],
                _r4(dc_upper[i]), _r4(dc_lower[i]),
                _r4(atr10[i]), atr_sq[i], _r4(pivot_high[i]),
                atr_decl[i], atr_slen[i], atr_sago[i],
                _r4(v.get('vcp_score')),  _r4(v.get('vcp_pivot')),
                _r4(v.get('vcp_vol_score')), _r4(v.get('vcp_depth_score')),
                v.get('vcp_num_legs'), v.get('vcp_is_signal', 0),
                _r4(as_raw['1m'][i]), _r4(as_raw['3m'][i]),
                _r4(as_raw['6m'][i]), _r4(as_raw['12m'][i]),
                _r4(rmv[5][i]), _r4(rmv[15][i]), _r4(rmv[50][i]),
                rs_line[i], rs_ema21[i], rs_sma50[i], rs_new_hi[i],
                rs_cross_above_sma50[i], rs_cross_below_sma50[i],
            ))

        # ── Write to DB ────────────────────────────────────────────────────────
        # Always purge existing rows first so stale bars are never left behind
        log(f"  Purging existing rows for {symbol} ...")
        delete_symbol(conn, symbol)

        BATCH = 1_000
        cur   = conn.cursor()
        for start in range(0, n, BATCH):
            cur.executemany(UPSERT_SQL, data[start : start + BATCH])
            conn.commit()
            if not silent:
                pct = min(start + BATCH, n) / n * 100
                print(f"  Inserted {min(start + BATCH, n):,}/{n:,} rows ({pct:.0f}%)", end="\r")

        cur.close()
        conn.close()

        elapsed = time.time() - t0
        log(f"\n  Done -- {n:,} rows in {elapsed:.1f}s")
        if not silent:
            print()
            print("  Sample query:")
            print(f"    SELECT o.bar_date, o.close, s.sma_50, s.rsi_14, s.vcp_score")
            print(f"    FROM   daily_ohlcv o")
            print(f"    INNER  JOIN daily_studies s ON s.symbol=o.symbol AND s.bar_date=o.bar_date")
            print(f"    WHERE  o.symbol = '{symbol}'")
            print(f"    ORDER  BY o.bar_date;")

        return {'symbol': symbol, 'bars': n, 'elapsed': elapsed, 'error': None}

    except Exception as exc:
        elapsed = time.time() - t0
        if not silent:
            raise
        return {'symbol': symbol, 'bars': 0, 'elapsed': elapsed, 'error': str(exc)}


BULK_QUERY = """
    SELECT ticker FROM leo.tickers
    WHERE last_day_volume > 500000
      AND type = 'CS'
      AND last_day_close > 5
      OR ticker = 'SPY'
    ORDER BY ticker
"""

DEFAULT_WORKERS = 8   # safe default — each worker holds one DB connection


def get_bulk_symbols(conn) -> list:
    cur = conn.cursor()
    cur.execute(BULK_QUERY)
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols


def _worker(args):
    """Target for each thread: unpack tuple and call run() in silent mode."""
    symbol, replace, bm_prices = args
    return run(symbol, replace, silent=True, benchmark_prices=bm_prices)


def run_bulk(symbols: list, replace: bool, workers: int):
    total     = len(symbols)
    done      = 0
    failed    = []
    lock      = threading.Lock()
    t0        = time.time()

    print(f"Bulk run: {total} symbols  |  workers={workers}  |  replace={replace}")
    print(f"  Pre-fetching {RS_BENCHMARK} benchmark prices ...")
    bm_conn = get_conn()
    bm_prices = fetch_benchmark(bm_conn)
    bm_conn.close()
    print(f"  {RS_BENCHMARK}: {len(bm_prices):,} dates loaded")
    print("-" * 60)

    def progress(result):
        nonlocal done
        with lock:
            done += 1
            status = "OK " if result['error'] is None else "ERR"
            eta_s  = ""
            elapsed = time.time() - t0
            if done > 1:
                rate   = done / elapsed
                remain = (total - done) / rate
                eta_s  = f"  ETA {remain:.0f}s"
            bars   = f"{result['bars']:,} bars" if result['error'] is None else result['error'][:40]
            print(f"  [{status}] {done:>4}/{total}  {result['symbol']:<8}  {bars}{eta_s}")
            if result['error']:
                failed.append(result)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, (sym, replace, bm_prices)): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                sym    = futures[future]
                result = {'symbol': sym, 'bars': 0, 'elapsed': 0, 'error': str(exc)}
            progress(result)

    elapsed = time.time() - t0
    ok      = total - len(failed)
    print("-" * 60)
    print(f"Done: {ok}/{total} succeeded  |  {len(failed)} failed  |  {elapsed:.1f}s total")
    if failed:
        print("\nFailed symbols:")
        for r in failed:
            print(f"  {r['symbol']}: {r['error']}")

    # Cross-sectional AS ranking requires all symbols to be present
    print()
    try:
        rank_conn = get_conn()
        run_as_ranking(rank_conn)
        rank_conn.close()
    except Exception as exc:
        print(f"  WARNING: AS ranking failed: {exc}")


def run_as_ranking(conn, verbose: bool = True):
    """
    Cross-sectional Absolute Strength percentile ranking.

    For every bar_date that has at least one as_raw_* value, each symbol's raw
    % change is ranked against all other symbols on that same date using
    MySQL's PERCENT_RANK() window function.  The resulting 1–99 integer is
    written back to as_1m / as_3m / as_6m / as_12m.

    Formula:  rating = ROUND(PERCENT_RANK() * 100), clamped to [1, 99]
      • Bottom stock  → PERCENT_RANK ≈ 0.00  →  rating  1
      • 87th pct      → PERCENT_RANK ≈ 0.87  →  rating 87
      • Top stock     → PERCENT_RANK = 1.00  →  rating 99 (clamped)

    Requires MySQL 8.0+ (window functions).
    """
    if verbose:
        print("  Computing AS cross-sectional percentile rankings ...")

    cur = conn.cursor()

    # Build rankings into a temp table using window functions partitioned by date
    cur.execute("DROP TEMPORARY TABLE IF EXISTS _as_ranks")
    cur.execute("""
        CREATE TEMPORARY TABLE _as_ranks ENGINE=InnoDB AS
        SELECT
            symbol,
            bar_date,
            LEAST(99, GREATEST(1, ROUND(
                PERCENT_RANK() OVER (PARTITION BY bar_date ORDER BY as_raw_1m)  * 100
            ))) AS r1m,
            LEAST(99, GREATEST(1, ROUND(
                PERCENT_RANK() OVER (PARTITION BY bar_date ORDER BY as_raw_3m)  * 100
            ))) AS r3m,
            LEAST(99, GREATEST(1, ROUND(
                PERCENT_RANK() OVER (PARTITION BY bar_date ORDER BY as_raw_6m)  * 100
            ))) AS r6m,
            LEAST(99, GREATEST(1, ROUND(
                PERCENT_RANK() OVER (PARTITION BY bar_date ORDER BY as_raw_12m) * 100
            ))) AS r12m
        FROM daily_studies
        WHERE as_raw_1m IS NOT NULL
    """)

    cur.execute("""
        UPDATE daily_studies ds
        INNER JOIN _as_ranks r ON ds.symbol = r.symbol AND ds.bar_date = r.bar_date
        SET ds.as_1m  = r.r1m,
            ds.as_3m  = r.r3m,
            ds.as_6m  = r.r6m,
            ds.as_12m = r.r12m
    """)
    conn.commit()

    cur.execute("DROP TEMPORARY TABLE IF EXISTS _as_ranks")
    cur.close()

    if verbose:
        print("  AS ranking complete.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Calculate technical studies and save to daily_studies.\n"
            "If --symbol is omitted all qualifying tickers from the tickers\n"
            "table (volume>500k, type=CS, close>5) are processed in parallel."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Single ticker (e.g. AAPL). Omit to run bulk.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        default=False,
        help="(Kept for backward compatibility — every run now purges before inserting.)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel worker threads for bulk run (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--rank-only",
        action="store_true",
        default=False,
        help=(
            "Skip indicator calculation; only (re-)compute AS percentile rankings "
            "from the as_raw_* values already stored in daily_studies."
        ),
    )
    args = parser.parse_args()

    if args.rank_only:
        conn = get_conn()
        ensure_table(conn)
        run_as_ranking(conn)
        conn.close()
    elif args.symbol:
        # Single-symbol mode: verbose, raises on error
        run(args.symbol.upper().strip(), args.replace, silent=False)
    else:
        # Bulk mode: parallel, one DB connection per worker thread
        conn    = get_conn()
        ensure_table(conn)          # create table once before threads start
        symbols = get_bulk_symbols(conn)
        conn.close()

        if not symbols:
            print("No qualifying symbols found.")
            sys.exit(0)

        run_bulk(symbols, args.replace, args.workers)


if __name__ == "__main__":
    main()
