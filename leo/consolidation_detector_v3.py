"""
=============================================================================
EXHAUSTIVE CONSOLIDATION ZONE DETECTOR  v3
=============================================================================
Connects to MySQL (leo.daily_ohlcv) or reads CSV. Detects tight horizontal
consolidation zones using 15 detection passes + quality-weighted fusion.

v3 improvements over v2 (from visual analysis of reference patterns):
  1. BAR COMPRESSION pass — individual bars must shrink inside zone
  2. PRECEDING TREND pass — zone should follow a directional move
  3. BODY OVERLAP pass — consecutive bar bodies must overlap heavily
  4. VOLUME CONTRACTION scoring — volume must dry up inside zone
  5. TIGHT BOUNDARY fusion — uses 10th/90th percentile, not median
  6. ADJACENT ZONE MERGING — zones separated by ≤2 bars get merged
  7. RELATIVE VOLATILITY — zone ATR vs surrounding ATR ratio
  8. WIDER SCORE RANGE — better separation of good vs marginal zones

Requirements:
    pip install pandas numpy scipy scikit-learn mysql-connector-python matplotlib
=============================================================================
"""

import warnings
warnings.filterwarnings("ignore")

import os, sys, argparse
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
from scipy.stats import entropy as scipy_entropy, gaussian_kde
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler


# ──────────────────────────────────────────────────────────────────────
# Method quality weights
# ──────────────────────────────────────────────────────────────────────
METHOD_WEIGHTS = {
    # Tier 1 — highly discriminating statistical signals
    "stationarity":     1.0,
    "entropy":          1.0,
    "kde_cluster":      1.0,
    "linreg_flat":      0.9,
    # Tier 1b — new v3 structural signals (very pattern-specific)
    "bar_compression":  1.0,
    "body_overlap":     0.9,
    "preceding_trend":  0.8,
    # Tier 2 — moderate signals
    "bbw_squeeze":      0.7,
    "atr_squeeze":      0.7,
    "ha_doji":          0.7,
    "dbscan":           0.7,
    # Tier 3 — weak / correlated trio
    "fractal_box":      0.4,
    "range_bars":       0.4,
    "volume_flat":      0.4,
    # Confirmation
    "mtf_2x":           0.5,
    "mtf_3x":           0.5,
    "breakout":         0.6,
}


# ──────────────────────────────────────────────────────────────────────
# Data Structures
# ──────────────────────────────────────────────────────────────────────
@dataclass
class ConsolidationZone:
    start_idx: int
    end_idx: int
    high: float
    low: float
    method: str
    confidence: float = 0.0
    num_bars: int = 0
    range_pct: float = 0.0
    atr_ratio: float = 0.0

    def __post_init__(self):
        self.num_bars = self.end_idx - self.start_idx + 1
        mid = (self.high + self.low) / 2 if (self.high + self.low) > 0 else 1
        self.range_pct = (self.high - self.low) / mid * 100

    def overlaps(self, other, min_frac=0.3):
        os = max(self.start_idx, other.start_idx)
        oe = min(self.end_idx, other.end_idx)
        ol = max(0, oe - os + 1)
        ml = min(self.num_bars, other.num_bars)
        return ml > 0 and (ol / ml) >= min_frac


@dataclass
class FusedZone:
    start_idx: int
    end_idx: int
    high: float
    low: float
    num_bars: int
    range_pct: float
    atr_ratio: float
    vote_count: int
    weighted_votes: float
    methods: List[str] = field(default_factory=list)
    composite_score: float = 0.0
    # v3 quality metrics
    bar_compression_ratio: float = 0.0   # avg bar range inside / outside
    body_overlap_score: float = 0.0       # avg body overlap fraction
    volume_contraction: float = 0.0       # avg volume ratio inside/outside
    relative_volatility: float = 0.0      # zone atr / surrounding atr
    preceding_trend_strength: float = 0.0
    # breakout
    breakout_confirmed: bool = False
    breakout_direction: str = ""
    breakout_magnitude: float = 0.0

    def to_dict(self):
        return {k: (round(v, 4) if isinstance(v, float) else v)
                for k, v in self.__dict__.items()}


# ──────────────────────────────────────────────────────────────────────
# Detector
# ──────────────────────────────────────────────────────────────────────
class ConsolidationDetector:

    def __init__(self, df, min_bars=4, max_bars=60, max_range_atr=2.5,
                 atr_period=14, num_iterations=30,
                 compression_floor=0.85, overlap_floor=0.25, rel_vol_floor=0.80):
        self.df = df.copy().reset_index(drop=True)
        self._normalize_columns()
        self.n = len(self.df)
        self.min_bars = min_bars
        self.max_bars = max_bars
        self.max_range_atr = max_range_atr
        self.atr_period = atr_period
        self.num_iterations = num_iterations
        self.compression_floor = compression_floor
        self.overlap_floor = overlap_floor
        self.rel_vol_floor = rel_vol_floor
        self._compute_atr()
        self._compute_derived()

        median_atr_pct = (self.df["atr"] / self.df["close"] * 100).median()
        self.max_range_pct = median_atr_pct * self.max_range_atr
        print(f"  Adaptive max_range_pct = {self.max_range_pct:.2f}% "
              f"(ATR% {median_atr_pct:.2f}% × {self.max_range_atr})")

    def _normalize_columns(self):
        col_map = {}
        for c in self.df.columns:
            cl = c.lower().strip()
            if cl in ("open", "high", "low", "close", "volume"):
                col_map[c] = cl
        self.df.rename(columns=col_map, inplace=True)
        for col in ("open", "high", "low", "close"):
            assert col in self.df.columns, f"Missing column: {col}"
        if "volume" not in self.df.columns:
            self.df["volume"] = 0

    def _compute_atr(self):
        h, l, c = self.df["high"], self.df["low"], self.df["close"]
        tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
        self.df["tr"] = tr
        self.df["atr"] = tr.rolling(self.atr_period, min_periods=1).mean()

    def _compute_derived(self):
        self.df["body_mid"] = (self.df["open"] + self.df["close"]) / 2
        self.df["candle_mid"] = (self.df["high"] + self.df["low"]) / 2
        self.df["body_size"] = (self.df["close"] - self.df["open"]).abs()
        self.df["candle_range"] = self.df["high"] - self.df["low"]
        self.df["body_top"] = self.df[["open", "close"]].max(axis=1)
        self.df["body_bot"] = self.df[["open", "close"]].min(axis=1)
        self.df["returns"] = self.df["close"].pct_change().fillna(0)
        self.df["vol_ma20"] = self.df["volume"].rolling(20, min_periods=1).mean()

    def _zone_stats(self, s, e):
        seg = self.df.iloc[s:e + 1]
        h, l = seg["high"].max(), seg["low"].min()
        mid = (h + l) / 2 if (h + l) > 0 else 1
        rpct = (h - l) / mid * 100
        avg_atr = seg["atr"].mean()
        atr_ratio = (h - l) / avg_atr if avg_atr > 0 else 999
        return h, l, rpct, atr_ratio

    def _is_valid(self, s, e):
        nbars = e - s + 1
        if nbars < self.min_bars or nbars > self.max_bars:
            return False
        _, _, rpct, atr_ratio = self._zone_stats(s, e)
        return rpct <= self.max_range_pct and atr_ratio <= self.max_range_atr

    def _make_zone(self, s, e, method, confidence=0.5):
        if not self._is_valid(s, e):
            return None
        h, l, rpct, atr_ratio = self._zone_stats(s, e)
        return ConsolidationZone(start_idx=s, end_idx=e, high=h, low=l,
                                 method=method, confidence=confidence,
                                 atr_ratio=atr_ratio)

    def _contiguous_groups(self, mask, min_len=None):
        if min_len is None: min_len = self.min_bars
        groups, in_g, start = [], False, 0
        for i in range(len(mask)):
            if mask.iloc[i] == 1 and not in_g:
                start, in_g = i, True
            elif mask.iloc[i] == 0 and in_g:
                if i - start >= min_len: groups.append((start, i - 1))
                in_g = False
        if in_g and len(mask) - start >= min_len:
            groups.append((start, len(mask) - 1))
        return groups

    # ══════════════════════════════════════════════════════════════════
    # PASS 1 — ATR Squeeze
    # ══════════════════════════════════════════════════════════════════
    def pass_atr_squeeze(self):
        zones = []
        for lb in np.unique(np.linspace(10, 60, self.num_iterations // 2, dtype=int)):
            atr_ma = self.df["atr"].rolling(lb, min_periods=1).mean()
            ratio = self.df["atr"] / atr_ma.replace(0, np.nan)
            for th in np.linspace(0.30, 0.75, self.num_iterations // 3):
                mask = (ratio < th).astype(int)
                for s, e in self._contiguous_groups(mask):
                    z = self._make_zone(s, e, "atr_squeeze", 0.3 + 0.7 * (1 - th))
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 2 — Bollinger Band Width Squeeze
    # ══════════════════════════════════════════════════════════════════
    def pass_bbw_squeeze(self):
        zones = []
        for period in np.unique(np.linspace(10, 40, self.num_iterations // 3, dtype=int)):
            sma = self.df["close"].rolling(period, min_periods=1).mean()
            std = self.df["close"].rolling(period, min_periods=1).std()
            bbw = (2 * std / sma.replace(0, np.nan)) * 100
            for pctile in np.linspace(5, 20, self.num_iterations // 3):
                cutoff = np.nanpercentile(bbw.dropna(), pctile)
                mask = (bbw < cutoff).astype(int)
                for s, e in self._contiguous_groups(mask):
                    z = self._make_zone(s, e, "bbw_squeeze", 0.5 + 0.5 * (1 - pctile / 100))
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 3 — Fractal Box (ATR-relative)
    # ══════════════════════════════════════════════════════════════════
    def pass_fractal_box(self):
        zones = []
        for w in np.unique(np.linspace(self.min_bars, min(self.max_bars, 50),
                                       self.num_iterations, dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 4)):
                end = start + w - 1
                h, l, rpct, atr_ratio = self._zone_stats(start, end)
                if atr_ratio < 1.5 and rpct <= self.max_range_pct:
                    conf = max(0, min(1, 1 - atr_ratio / self.max_range_atr))
                    z = self._make_zone(start, end, "fractal_box", conf)
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 4 — Heikin-Ashi Doji Clustering
    # ══════════════════════════════════════════════════════════════════
    def pass_ha_doji(self):
        ha = self._heikin_ashi()
        body_ratio = ha["body_size"] / ha["candle_range"].replace(0, np.nan)
        zones = []
        for th in np.linspace(0.15, 0.40, self.num_iterations // 3):
            mask = (body_ratio < th).astype(int)
            for mr in np.unique(np.linspace(3, 8, 4, dtype=int)):
                for s, e in self._contiguous_groups(mask, min_len=mr):
                    z = self._make_zone(s, e, "ha_doji", 0.5 + 0.3 * (1 - th))
                    if z: zones.append(z)
        return zones

    def _heikin_ashi(self):
        ha = pd.DataFrame(index=self.df.index)
        ha["close"] = (self.df["open"] + self.df["high"] + self.df["low"] + self.df["close"]) / 4
        ha["open"] = 0.0
        ha.loc[0, "open"] = (self.df["open"].iloc[0] + self.df["close"].iloc[0]) / 2
        for i in range(1, len(ha)):
            ha.loc[i, "open"] = (ha["open"].iloc[i-1] + ha["close"].iloc[i-1]) / 2
        ha["high"] = pd.concat([ha["open"], ha["close"], self.df["high"]], axis=1).max(axis=1)
        ha["low"] = pd.concat([ha["open"], ha["close"], self.df["low"]], axis=1).min(axis=1)
        ha["body_size"] = (ha["close"] - ha["open"]).abs()
        ha["candle_range"] = ha["high"] - ha["low"]
        return ha

    # ══════════════════════════════════════════════════════════════════
    # PASS 5 — Volume Flat Zones
    # ══════════════════════════════════════════════════════════════════
    def pass_volume_flat(self):
        if self.df["volume"].sum() == 0: return []
        zones = []
        for w in np.unique(np.linspace(self.min_bars, min(self.max_bars, 40),
                                       self.num_iterations // 2, dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 3)):
                end = start + w - 1
                seg = self.df.iloc[start:end + 1]
                vol = seg["volume"]
                if vol.sum() == 0: continue
                vwap = (seg["candle_mid"] * vol).sum() / vol.sum()
                disp = np.sqrt(((seg["candle_mid"] - vwap)**2 * vol).sum() / vol.sum())
                disp_pct = disp / vwap * 100 if vwap > 0 else 999
                if disp_pct < 1.2:
                    z = self._make_zone(start, end, "volume_flat", max(0, 1 - disp_pct / 1.2))
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 6 — Range Bars
    # ══════════════════════════════════════════════════════════════════
    def pass_range_bars(self):
        zones = []
        for mult in np.linspace(0.5, 2.0, self.num_iterations // 3):
            i = 0
            while i < self.n:
                atr_val = self.df["atr"].iloc[i] * mult
                env_h = self.df["high"].iloc[i] + atr_val * 0.2
                env_l = self.df["low"].iloc[i] - atr_val * 0.2
                j = i + 1
                while j < self.n:
                    if self.df["high"].iloc[j] <= env_h and self.df["low"].iloc[j] >= env_l:
                        env_h = max(env_h, self.df["high"].iloc[j])
                        env_l = min(env_l, self.df["low"].iloc[j])
                        _, _, _, ar = self._zone_stats(i, j)
                        if ar > self.max_range_atr: break
                        j += 1
                    else: break
                if j - i >= self.min_bars:
                    z = self._make_zone(i, j - 1, "range_bars", 0.5)
                    if z: zones.append(z)
                i = max(i + 1, j - 1)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 7 — Stationarity (Variance Ratio)
    # ══════════════════════════════════════════════════════════════════
    def pass_stationarity(self):
        zones = []
        for w in np.unique(np.linspace(max(self.min_bars, 10), min(self.max_bars, 50),
                                       min(self.num_iterations, 15), dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 4)):
                end = start + w - 1
                seg = self.df["close"].iloc[start:end + 1].values
                if len(seg) < 8: continue
                rets = np.diff(seg)
                var1 = np.var(rets)
                if var1 == 0: continue
                k = max(2, len(rets) // 3)
                vark = np.var(seg[k:] - seg[:-k])
                vr = vark / (k * var1)
                if vr < 0.65:
                    z = self._make_zone(start, end, "stationarity", min(1, 1 - vr))
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 8 — KDE Clustering
    # ══════════════════════════════════════════════════════════════════
    def pass_kde_clustering(self):
        zones = []
        for w in np.unique(np.linspace(max(self.min_bars, 8), min(self.max_bars, 45),
                                       min(self.num_iterations, 12), dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 3)):
                end = start + w - 1
                seg = self.df["close"].iloc[start:end + 1].values
                if len(seg) < 6 or np.std(seg) == 0: continue
                try:
                    kde = gaussian_kde(seg)
                    x_grid = np.linspace(seg.min(), seg.max(), 100)
                    density = kde(x_grid)
                    pc = density.max() * (seg.max() - seg.min())
                    if pc < 2.0:
                        z = self._make_zone(start, end, "kde_cluster", min(1, 1 - pc / 2.5))
                        if z: zones.append(z)
                except: continue
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 9 — Shannon Entropy
    # ══════════════════════════════════════════════════════════════════
    def pass_entropy(self):
        zones = []
        returns = self.df["returns"]
        for w in np.unique(np.linspace(max(self.min_bars, 8), min(self.max_bars, 45),
                                       min(self.num_iterations, 15), dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 4)):
                end = start + w - 1
                seg_ret = returns.iloc[start:end + 1].values
                for nb in [5, 8, 12]:
                    hist, _ = np.histogram(seg_ret, bins=nb, density=True)
                    hist = hist / hist.sum() if hist.sum() > 0 else hist
                    hist = hist[hist > 0]
                    ent = scipy_entropy(hist)
                    max_ent = np.log(nb)
                    norm_ent = ent / max_ent if max_ent > 0 else 1
                    if norm_ent < 0.50:
                        z = self._make_zone(start, end, "entropy", max(0, 1 - norm_ent))
                        if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 10 — DBSCAN
    # ══════════════════════════════════════════════════════════════════
    def pass_dbscan(self):
        zones = []
        X = np.column_stack([np.arange(self.n), self.df["body_mid"].values])
        X_sc = StandardScaler().fit_transform(X)
        for eps in np.linspace(0.3, 1.2, min(self.num_iterations, 8)):
            for ms in [3, 4, 5, 6]:
                try:
                    labels = DBSCAN(eps=eps, min_samples=ms).fit(X_sc).labels_
                    for lab in set(labels):
                        if lab == -1: continue
                        idxs = np.where(labels == lab)[0]
                        s, e = idxs.min(), idxs.max()
                        z = self._make_zone(s, e, "dbscan", 0.5)
                        if z: zones.append(z)
                except: continue
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 11 — LinReg Flat
    # ══════════════════════════════════════════════════════════════════
    def pass_linreg_flat(self):
        zones = []
        for w in np.unique(np.linspace(max(self.min_bars, 6), min(self.max_bars, 45),
                                       min(self.num_iterations, 15), dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 3)):
                end = start + w - 1
                y = self.df["close"].iloc[start:end + 1].values
                x = np.arange(len(y))
                if len(y) < 4: continue
                xm, ym = x.mean(), y.mean()
                ss_xy = ((x - xm) * (y - ym)).sum()
                ss_xx = ((x - xm)**2).sum()
                if ss_xx == 0: continue
                slope = ss_xy / ss_xx
                norm_slope = abs(slope) / ym * 100 if ym > 0 else 999
                if norm_slope < 0.05:
                    z = self._make_zone(start, end, "linreg_flat", min(1, 1 - norm_slope / 0.1))
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # PASS 12 — Multi-Timeframe
    # ══════════════════════════════════════════════════════════════════
    def pass_multi_timeframe(self):
        zones = []
        for factor in [2, 3]:
            agg = self._aggregate(factor)
            if len(agg) < self.min_bars: continue
            for w in range(self.min_bars, min(len(agg), self.max_bars // factor + 1)):
                for start in range(0, len(agg) - w + 1):
                    end = start + w - 1
                    seg = agg.iloc[start:end + 1]
                    h, l = seg["high"].max(), seg["low"].min()
                    mid = (h + l) / 2 if (h + l) > 0 else 1
                    rpct = (h - l) / mid * 100
                    if rpct <= self.max_range_pct and (h - l) < seg["tr"].mean() * 1.8:
                        os = start * factor
                        oe = min((end + 1) * factor - 1, self.n - 1)
                        z = self._make_zone(os, oe, f"mtf_{factor}x",
                                            max(0, 1 - rpct / self.max_range_pct) * 0.6)
                        if z: zones.append(z)
        return zones

    def _aggregate(self, factor):
        rows = []
        for i in range(self.n // factor):
            seg = self.df.iloc[i * factor:(i + 1) * factor]
            rows.append({"open": seg["open"].iloc[0], "high": seg["high"].max(),
                          "low": seg["low"].min(), "close": seg["close"].iloc[-1],
                          "volume": seg["volume"].sum()})
        agg = pd.DataFrame(rows)
        agg["tr"] = agg["high"] - agg["low"]
        return agg

    # ══════════════════════════════════════════════════════════════════
    # ★ v3 PASS 13 — BAR COMPRESSION (individual bars shrink inside zone)
    # ══════════════════════════════════════════════════════════════════
    def pass_bar_compression(self):
        """
        Detects windows where individual bar ranges are significantly
        smaller than surrounding bars. The key visual signature:
        bars THEMSELVES get tiny, not just contained in a range.
        """
        zones = []
        bar_range = self.df["candle_range"]

        for w in np.unique(np.linspace(self.min_bars, min(self.max_bars, 40),
                                       self.num_iterations, dtype=int)):
            for start in range(w, self.n - w + 1, max(1, w // 4)):
                end = start + w - 1
                if end >= self.n: break

                # Bar ranges inside zone
                inside = bar_range.iloc[start:end + 1].mean()

                # Bar ranges in lookback window before zone
                lb_start = max(0, start - w * 2)
                before = bar_range.iloc[lb_start:start].mean()

                # Bar ranges after zone
                la_end = min(self.n, end + 1 + w)
                after_vals = bar_range.iloc[end + 1:la_end]
                after = after_vals.mean() if len(after_vals) > 0 else before

                surrounding = (before + after) / 2 if after > 0 else before
                if surrounding == 0: continue

                ratio = inside / surrounding
                # Bars inside should be < 60% of surrounding
                if ratio < 0.60:
                    conf = min(1.0, (0.60 - ratio) / 0.40)
                    z = self._make_zone(start, end, "bar_compression", conf)
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # ★ v3 PASS 14 — BODY OVERLAP (consecutive bar bodies overlap)
    # ══════════════════════════════════════════════════════════════════
    def pass_body_overlap(self):
        """
        True consolidation = bar bodies stacked on top of each other.
        Measures pairwise overlap of consecutive bar bodies.
        """
        zones = []
        bt = self.df["body_top"]
        bb = self.df["body_bot"]

        for w in np.unique(np.linspace(self.min_bars, min(self.max_bars, 35),
                                       self.num_iterations, dtype=int)):
            for start in range(0, self.n - w + 1, max(1, w // 4)):
                end = start + w - 1
                seg_bt = bt.iloc[start:end + 1].values
                seg_bb = bb.iloc[start:end + 1].values

                if len(seg_bt) < 3: continue

                overlaps = []
                for i in range(len(seg_bt) - 1):
                    # Overlap between bar i and bar i+1
                    ovlp_top = min(seg_bt[i], seg_bt[i + 1])
                    ovlp_bot = max(seg_bb[i], seg_bb[i + 1])
                    ovlp = max(0, ovlp_top - ovlp_bot)
                    # Normalize by average body size
                    body_a = seg_bt[i] - seg_bb[i]
                    body_b = seg_bt[i + 1] - seg_bb[i + 1]
                    avg_body = (body_a + body_b) / 2
                    if avg_body > 0:
                        overlaps.append(ovlp / avg_body)
                    else:
                        overlaps.append(1.0)  # both dojis = perfect overlap

                if not overlaps: continue
                avg_overlap = np.mean(overlaps)
                # High overlap (>0.5) = bodies heavily stacked
                if avg_overlap > 0.50:
                    conf = min(1.0, (avg_overlap - 0.50) / 0.50)
                    z = self._make_zone(start, end, "body_overlap", conf)
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # ★ v3 PASS 15 — PRECEDING TREND (zone follows directional move)
    # ══════════════════════════════════════════════════════════════════
    def pass_preceding_trend(self):
        """
        Consolidation zones that follow a strong directional move are
        the most tradeable (pause/continuation or reversal patterns).
        This pass finds flat zones preceded by significant price movement.
        """
        zones = []
        lookbacks = [5, 8, 12, 15, 20]

        for w in np.unique(np.linspace(self.min_bars, min(self.max_bars, 35),
                                       self.num_iterations // 2, dtype=int)):
            for start in range(20, self.n - w + 1, max(1, w // 3)):
                end = start + w - 1
                if not self._is_valid(start, end): continue

                # Check for preceding trend in multiple lookbacks
                best_trend = 0
                for lb in lookbacks:
                    if start - lb < 0: continue
                    pre_ret = abs(self.df["close"].iloc[start] - self.df["close"].iloc[start - lb])
                    pre_atr = self.df["atr"].iloc[max(0, start - lb):start].mean()
                    if pre_atr > 0:
                        trend_strength = pre_ret / pre_atr
                        best_trend = max(best_trend, trend_strength)

                # Trend > 2 ATR before zone = interesting
                if best_trend > 2.0:
                    conf = min(1.0, (best_trend - 2.0) / 4.0)
                    z = self._make_zone(start, end, "preceding_trend", conf)
                    if z: zones.append(z)
        return zones

    # ══════════════════════════════════════════════════════════════════
    # QUALITY METRICS (computed on fused zones)
    # ══════════════════════════════════════════════════════════════════
    def _compute_quality_metrics(self, zone: FusedZone):
        """Compute v3 quality metrics for a fused zone."""
        s, e = zone.start_idx, zone.end_idx
        seg = self.df.iloc[s:e + 1]

        # 1. Bar compression ratio
        inside_range = seg["candle_range"].mean()
        lb = max(0, s - (e - s + 1) * 2)
        la = min(self.n, e + 1 + (e - s + 1))
        before_range = self.df["candle_range"].iloc[lb:s].mean() if s > lb else inside_range
        after_range = self.df["candle_range"].iloc[e + 1:la].mean() if la > e + 1 else inside_range
        surr = (before_range + after_range) / 2 if (before_range + after_range) > 0 else 1
        zone.bar_compression_ratio = inside_range / surr if surr > 0 else 1.0

        # 2. Body overlap score
        bt = seg["body_top"].values
        bb = seg["body_bot"].values
        overlaps = []
        for i in range(len(bt) - 1):
            ovlp = max(0, min(bt[i], bt[i+1]) - max(bb[i], bb[i+1]))
            avg_b = ((bt[i] - bb[i]) + (bt[i+1] - bb[i+1])) / 2
            overlaps.append(ovlp / avg_b if avg_b > 0 else 1.0)
        zone.body_overlap_score = np.mean(overlaps) if overlaps else 0.0

        # 3. Volume contraction
        inside_vol = seg["volume"].mean()
        before_vol = self.df["volume"].iloc[lb:s].mean() if s > lb else inside_vol
        zone.volume_contraction = inside_vol / before_vol if before_vol > 0 else 1.0

        # 4. Relative volatility
        inside_atr = seg["tr"].mean()
        before_atr = self.df["tr"].iloc[lb:s].mean() if s > lb else inside_atr
        after_atr = self.df["tr"].iloc[e + 1:la].mean() if la > e + 1 else inside_atr
        surr_atr = (before_atr + after_atr) / 2 if (before_atr + after_atr) > 0 else 1
        zone.relative_volatility = inside_atr / surr_atr if surr_atr > 0 else 1.0

        # 5. Preceding trend strength
        best = 0
        for lb_len in [5, 8, 12, 15]:
            if s - lb_len < 0: continue
            move = abs(self.df["close"].iloc[s] - self.df["close"].iloc[s - lb_len])
            pre_atr = self.df["atr"].iloc[max(0, s - lb_len):s].mean()
            if pre_atr > 0:
                best = max(best, move / pre_atr)
        zone.preceding_trend_strength = best

    # ══════════════════════════════════════════════════════════════════
    # BREAKOUT CHECK
    # ══════════════════════════════════════════════════════════════════
    def _check_breakout(self, zone: FusedZone, lookforward=10):
        end = zone.end_idx
        if end + 1 >= self.n:
            return
        fwd_end = min(end + lookforward, self.n - 1)
        fwd = self.df.iloc[end + 1:fwd_end + 1]
        if len(fwd) == 0: return

        zone_range = zone.high - zone.low
        avg_atr = self.df["atr"].iloc[max(0, end - 5):end + 1].mean()
        threshold = max(zone_range * 1.0, avg_atr * 0.8)

        max_up = fwd["high"].max() - zone.high
        max_down = zone.low - fwd["low"].min()

        if max_up > threshold and max_up >= max_down:
            zone.breakout_confirmed = True
            zone.breakout_direction = "up"
            zone.breakout_magnitude = max_up / avg_atr if avg_atr > 0 else 0
        elif max_down > threshold:
            zone.breakout_confirmed = True
            zone.breakout_direction = "down"
            zone.breakout_magnitude = max_down / avg_atr if avg_atr > 0 else 0

    # ══════════════════════════════════════════════════════════════════
    # FUSION v3 — tight boundaries + quality metrics + wider scoring
    # ══════════════════════════════════════════════════════════════════
    def fuse_zones(self, all_zones, min_weighted_votes=2.5):
        if not all_zones: return []

        all_zones.sort(key=lambda z: (z.start_idx, z.end_idx))

        # Cluster overlapping zones
        clusters = []
        current = [all_zones[0]]
        for z in all_zones[1:]:
            if any(z.overlaps(c) for c in current):
                current.append(z)
            else:
                clusters.append(current)
                current = [z]
        clusters.append(current)

        fused = []
        for cluster in clusters:
            methods = list(set(z.method for z in cluster))
            weighted = sum(METHOD_WEIGHTS.get(m, 0.5) for m in methods)
            if weighted < min_weighted_votes: continue

            # ★ v3: TIGHT boundaries — use 10th/90th percentile
            starts = [z.start_idx for z in cluster]
            ends = [z.end_idx for z in cluster]
            highs = [z.high for z in cluster]
            lows = [z.low for z in cluster]

            fs = int(np.percentile(starts, 40))
            fe = int(np.percentile(ends, 60))
            fh = np.percentile(highs, 60)   # not max — tighter top
            fl = np.percentile(lows, 40)     # not min — tighter bottom

            if fe <= fs: fe = fs + self.min_bars - 1
            if fe >= self.n: fe = self.n - 1

            nbars = fe - fs + 1
            mid = (fh + fl) / 2 if (fh + fl) > 0 else 1
            rpct = (fh - fl) / mid * 100
            avg_atr = self.df["atr"].iloc[fs:fe + 1].mean()
            atr_ratio = (fh - fl) / avg_atr if avg_atr > 0 else 999

            avg_conf = np.mean([z.confidence for z in cluster])

            fz = FusedZone(
                start_idx=fs, end_idx=fe, high=fh, low=fl,
                num_bars=nbars, range_pct=rpct, atr_ratio=atr_ratio,
                vote_count=len(methods), weighted_votes=weighted,
                methods=methods, composite_score=0,
            )
            fused.append(fz)

        # ★ v3: Merge adjacent zones (gap ≤ 2 bars)
        fused = self._merge_adjacent(fused)

        # Compute quality metrics for each zone
        for fz in fused:
            self._compute_quality_metrics(fz)
            self._check_breakout(fz)

        # ★ v3: Compute final score with wider dynamic range
        for fz in fused:
            max_w = sum(METHOD_WEIGHTS.values())
            vote_score = fz.weighted_votes / max_w

            tightness = max(0, 1 - fz.atr_ratio / self.max_range_atr)

            # Quality bonuses (0 to 1 each)
            compression_bonus = max(0, 1 - fz.bar_compression_ratio) if fz.bar_compression_ratio < 1 else 0
            overlap_bonus = fz.body_overlap_score
            vol_bonus = max(0, 1 - fz.volume_contraction) if fz.volume_contraction < 1 else 0
            rel_vol_bonus = max(0, 1 - fz.relative_volatility) if fz.relative_volatility < 1 else 0
            trend_bonus = min(1.0, fz.preceding_trend_strength / 5.0)
            breakout_bonus = 0.15 if fz.breakout_confirmed else 0

            # Weighted composite — wider range
            raw_score = (
                vote_score          * 0.20 +
                tightness           * 0.15 +
                compression_bonus   * 0.15 +
                overlap_bonus       * 0.12 +
                vol_bonus           * 0.10 +
                rel_vol_bonus       * 0.08 +
                trend_bonus         * 0.10 +
                breakout_bonus      * 0.10
            )

            # Stretch to wider range: apply power curve
            fz.composite_score = min(1.0, raw_score ** 0.7)

        # ★ v3: Quality floor — at least 2 of 3 quality signals must be present
        filtered = []
        for fz in fused:
            if fz.range_pct > self.max_range_pct:
                continue
            quality_signals = 0
            if fz.bar_compression_ratio < self.compression_floor: quality_signals += 1
            if fz.body_overlap_score > self.overlap_floor: quality_signals += 1
            if fz.relative_volatility < self.rel_vol_floor: quality_signals += 1
            if quality_signals >= 2:
                filtered.append(fz)
        fused = filtered

        fused.sort(key=lambda f: f.composite_score, reverse=True)
        return fused

    def _merge_adjacent(self, zones, max_gap=2):
        """Merge zones separated by ≤ max_gap bars, but only if the
        merged result still passes range validation."""
        if len(zones) < 2: return zones
        zones.sort(key=lambda z: z.start_idx)
        merged = [zones[0]]
        for z in zones[1:]:
            prev = merged[-1]
            gap = z.start_idx - prev.end_idx - 1
            if gap <= max_gap:
                # Check if merge would still be valid
                new_h = max(prev.high, z.high)
                new_l = min(prev.low, z.low)
                new_end = max(prev.end_idx, z.end_idx)
                new_nbars = new_end - prev.start_idx + 1
                mid = (new_h + new_l) / 2
                new_rpct = (new_h - new_l) / mid * 100 if mid > 0 else 999
                avg_atr = self.df["atr"].iloc[prev.start_idx:new_end + 1].mean()
                new_atr_ratio = (new_h - new_l) / avg_atr if avg_atr > 0 else 999

                # Only merge if result stays within limits and isn't too wide
                if (new_rpct <= self.max_range_pct and
                    new_atr_ratio <= self.max_range_atr and
                    new_nbars <= self.max_bars):
                    prev.end_idx = new_end
                    prev.high = new_h
                    prev.low = new_l
                    prev.num_bars = new_nbars
                    prev.range_pct = new_rpct
                    prev.atr_ratio = new_atr_ratio
                    prev.vote_count = max(prev.vote_count, z.vote_count)
                    prev.weighted_votes = max(prev.weighted_votes, z.weighted_votes)
                    prev.methods = list(set(prev.methods + z.methods))
                else:
                    merged.append(z)
            else:
                merged.append(z)
        return merged

    # ══════════════════════════════════════════════════════════════════
    # RUN ALL
    # ══════════════════════════════════════════════════════════════════
    def run_all_passes(self, min_weighted_votes=2.5, verbose=True):
        passes = [
            ("ATR Squeeze",       self.pass_atr_squeeze),
            ("BBW Squeeze",       self.pass_bbw_squeeze),
            ("Fractal Box",       self.pass_fractal_box),
            ("HA Doji",           self.pass_ha_doji),
            ("Volume Flat",       self.pass_volume_flat),
            ("Range Bars",        self.pass_range_bars),
            ("Stationarity",      self.pass_stationarity),
            ("KDE Clustering",    self.pass_kde_clustering),
            ("Entropy",           self.pass_entropy),
            ("DBSCAN",            self.pass_dbscan),
            ("LinReg Flat",       self.pass_linreg_flat),
            ("Multi-TF",          self.pass_multi_timeframe),
            ("Bar Compression",   self.pass_bar_compression),
            ("Body Overlap",      self.pass_body_overlap),
            ("Preceding Trend",   self.pass_preceding_trend),
        ]

        all_zones = []
        for name, func in passes:
            if verbose: print(f"  Pass: {name}...", end=" ", flush=True)
            try:
                result = func()
                all_zones.extend(result)
                if verbose: print(f"{len(result)} raw")
            except Exception as e:
                if verbose: print(f"ERROR: {e}")

        if verbose:
            print(f"\n  Total raw: {len(all_zones)}")
            print(f"  Fusing (min_wt={min_weighted_votes})...")

        fused = self.fuse_zones(all_zones, min_weighted_votes)

        if verbose:
            bo = sum(1 for z in fused if z.breakout_confirmed)
            print(f"  Final zones: {len(fused)} ({bo} breakout-confirmed)\n")

        return fused


# ──────────────────────────────────────────────────────────────────────
# Data Loading
# ──────────────────────────────────────────────────────────────────────
def load_from_mysql(symbol="F", host="127.0.0.1", user="remote",
                    password="Chamba4347!", database="leo", port=3306, limit=None):
    import mysql.connector
    conn = mysql.connector.connect(host=host, user=user, password=password,
                                   database=database, port=port)
    if limit:
        query = """SELECT bar_date,open,high,low,close,volume FROM (
                     SELECT bar_date,open,high,low,close,volume FROM daily_ohlcv
                     WHERE symbol=%s ORDER BY bar_date DESC LIMIT %s
                   ) sub ORDER BY bar_date ASC"""
        params = [symbol, limit]
    else:
        query = """SELECT bar_date,open,high,low,close,volume FROM daily_ohlcv
                   WHERE symbol=%s ORDER BY bar_date ASC"""
        params = [symbol]
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    for c in ["open","high","low","close"]: df[c] = df[c].astype(float)
    df["volume"] = df["volume"].astype(int)
    return df


def load_from_csv(path):
    df = pd.read_csv(path)
    for c in df.columns:
        cl = c.lower().strip()
        if cl in ("open","high","low","close"): df[c] = df[c].astype(float)
    return df


# ──────────────────────────────────────────────────────────────────────
# Plotting v3
# ──────────────────────────────────────────────────────────────────────
def plot_zones(df, zones, symbol="", save_path="consolidation_zones_v3.png"):
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from matplotlib.gridspec import GridSpec

    fig = plt.figure(figsize=(28, 14))
    gs = GridSpec(4, 1, height_ratios=[3, 0.6, 0.6, 0.6], hspace=0.08)
    fig.patch.set_facecolor("#0a0a18")

    has_dates = "bar_date" in df.columns

    # ── Main chart ──
    ax = fig.add_subplot(gs[0])
    ax.set_facecolor("#0a0a18")

    for i in range(len(df)):
        o, h, l, c = [float(df[x].iloc[i]) for x in ("open","high","low","close")]
        color = "#26a69a" if c >= o else "#ef5350"
        ax.plot([i, i], [l, h], color=color, linewidth=0.5)
        ax.add_patch(patches.Rectangle(
            (i - 0.35, min(o, c)), 0.7, max(abs(c - o), 0.001),
            edgecolor=color, facecolor=color, alpha=0.9, linewidth=0.3))

    for z in zones:
        if z.breakout_confirmed:
            ec = "#00ff88" if z.breakout_direction == "up" else "#ff4444"
        else:
            ec = "#ff69b4"
        alpha = 0.12 + 0.30 * z.composite_score
        ax.add_patch(patches.Rectangle(
            (z.start_idx - 0.5, z.low), z.num_bars, z.high - z.low,
            linewidth=2, edgecolor=ec, facecolor=ec, alpha=alpha))

        parts = [f"S={z.composite_score:.2f}"]
        if z.breakout_confirmed:
            arrow = "↑" if z.breakout_direction == "up" else "↓"
            parts.append(f"{arrow}{z.breakout_magnitude:.1f}ATR")
        if has_dates:
            parts.insert(0, f"{df['bar_date'].iloc[z.start_idx]}")
        ax.text(z.start_idx, z.high, " ".join(parts),
                fontsize=5.5, color="white", va="bottom")

    ax.set_title(f"{symbol} — Consolidation Zones v3 ({len(zones)} zones)",
                 color="white", fontsize=14, fontweight="bold")
    ax.tick_params(colors="white", labelsize=7)
    ax.set_ylabel("Price", color="white", fontsize=9)
    for s in ax.spines.values(): s.set_color("#222")
    ax.set_xlim(-1, len(df) + 1)

    # ── Volume subplot ──
    ax_vol = fig.add_subplot(gs[1], sharex=ax)
    ax_vol.set_facecolor("#0a0a18")
    colors_vol = ["#26a69a" if df["close"].iloc[i] >= df["open"].iloc[i] else "#ef5350"
                  for i in range(len(df))]
    ax_vol.bar(range(len(df)), df["volume"], color=colors_vol, alpha=0.6, width=0.7)
    vol_ma = df["volume"].rolling(20, min_periods=1).mean()
    ax_vol.plot(range(len(df)), vol_ma, color="#ffffff", alpha=0.4, linewidth=0.8)
    ax_vol.set_ylabel("Vol", color="white", fontsize=8)
    ax_vol.tick_params(colors="white", labelsize=6)
    for s in ax_vol.spines.values(): s.set_color("#222")

    # ── ATR subplot ──
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=1).mean()

    ax_atr = fig.add_subplot(gs[2], sharex=ax)
    ax_atr.set_facecolor("#0a0a18")
    ax_atr.plot(range(len(df)), atr14, color="#ffa726", alpha=0.8, linewidth=0.8)
    ax_atr.set_ylabel("ATR", color="white", fontsize=8)
    ax_atr.tick_params(colors="white", labelsize=6)
    for s in ax_atr.spines.values(): s.set_color("#222")

    # ── Score heatmap subplot ──
    ax_heat = fig.add_subplot(gs[3], sharex=ax)
    ax_heat.set_facecolor("#0a0a18")
    score_line = np.zeros(len(df))
    for z in zones:
        for i in range(z.start_idx, min(z.end_idx + 1, len(df))):
            score_line[i] = max(score_line[i], z.composite_score)
    ax_heat.fill_between(range(len(df)), score_line, color="#ff69b4", alpha=0.5)
    ax_heat.set_ylabel("Score", color="white", fontsize=8)
    ax_heat.set_ylim(0, 1)
    ax_heat.tick_params(colors="white", labelsize=6)
    for s in ax_heat.spines.values(): s.set_color("#222")

    # X-axis dates on bottom
    if has_dates:
        step = max(1, len(df) // 25)
        ticks = list(range(0, len(df), step))
        ax_heat.set_xticks(ticks)
        ax_heat.set_xticklabels([str(df["bar_date"].iloc[i]) for i in ticks],
                                rotation=45, ha="right", fontsize=6, color="white")
    plt.setp(ax.get_xticklabels(), visible=False)
    plt.setp(ax_vol.get_xticklabels(), visible=False)
    plt.setp(ax_atr.get_xticklabels(), visible=False)

    plt.tight_layout()
    plt.savefig(save_path, dpi=180, facecolor="#0a0a18")
    plt.close()
    print(f"  Chart saved: {save_path}")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Consolidation Detector v3")
    p.add_argument("--symbol", default="F")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--user", default="remote")
    p.add_argument("--password", default="Chamba4347!")
    p.add_argument("--database", default="leo")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--csv", type=str, default=None)
    p.add_argument("--min-bars", type=int, default=4)
    p.add_argument("--max-bars", type=int, default=50)
    p.add_argument("--max-range-atr", type=float, default=2.0)
    p.add_argument("--min-weighted-votes", type=float, default=2.5)
    p.add_argument("--iterations", type=int, default=30)
    p.add_argument("--output", default="consolidation_zones_v3.png")
    args = p.parse_args()

    print("=" * 70)
    print("CONSOLIDATION ZONE DETECTOR v3")
    print("=" * 70)

    if args.csv:
        df = load_from_csv(args.csv)
        print(f"Loaded {len(df)} bars from {args.csv}")
    else:
        df = load_from_mysql(args.symbol, args.host, args.user, args.password,
                             args.database, args.port, args.limit)
        print(f"Loaded {len(df)} bars for {args.symbol}")

    if "bar_date" in df.columns:
        print(f"  Date range: {df['bar_date'].iloc[0]} → {df['bar_date'].iloc[-1]}")
    print(f"  Price range: {df['low'].min():.2f} — {df['high'].max():.2f}")
    print()

    det = ConsolidationDetector(df, min_bars=args.min_bars, max_bars=args.max_bars,
                                max_range_atr=args.max_range_atr,
                                num_iterations=args.iterations)
    zones = det.run_all_passes(min_weighted_votes=args.min_weighted_votes, verbose=True)

    print("DETECTED ZONES (ranked by composite score):")
    print("=" * 100)
    for i, z in enumerate(zones):
        d1 = df["bar_date"].iloc[z.start_idx] if "bar_date" in df.columns else z.start_idx
        d2 = df["bar_date"].iloc[z.end_idx] if "bar_date" in df.columns else z.end_idx
        bo = ""
        if z.breakout_confirmed:
            arrow = "↑" if z.breakout_direction == "up" else "↓"
            bo = f" ★ BO:{arrow}{z.breakout_magnitude:.1f}ATR"
        print(f"  {i+1:>2}. {d1} → {d2} ({z.num_bars}bars) "
              f"${z.low:.2f}-${z.high:.2f} ({z.range_pct:.1f}% {z.atr_ratio:.1f}ATR) "
              f"score={z.composite_score:.3f}{bo}")
        print(f"      compression={z.bar_compression_ratio:.2f} "
              f"overlap={z.body_overlap_score:.2f} "
              f"vol_contract={z.volume_contraction:.2f} "
              f"rel_vol={z.relative_volatility:.2f} "
              f"trend={z.preceding_trend_strength:.1f}ATR")
        if i < 3:
            print(f"      methods({z.vote_count}): {', '.join(sorted(z.methods))}")
        print()

    try:
        plot_zones(df, zones, symbol=args.symbol, save_path=args.output)
    except Exception as e:
        print(f"  Plot skipped: {e}")

    print(f"✓ {len(zones)} zones for {args.symbol}.")
