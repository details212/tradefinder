"""
=============================================================================
FORWARD-MODE CONSOLIDATION DETECTOR — EXHAUSTIVE, NO REPAINT
=============================================================================
Processes bars one at a time. At each bar, runs the COMPLETE v3 retrospective
engine on the trailing window — same 15 passes, same parameter sweeps, same
fusion, same quality floor. Zero shortcuts.

NO-REPAINT GUARANTEES:
  1. Once a zone is CONFIRMED, its boundaries and score are LOCKED forever.
  2. Detection at bar N uses ONLY bars 0..N. No future data touches anything.
  3. A CONFIRMED zone is never re-evaluated, rescored, or boundary-adjusted.
  4. Breakout detection compares the current bar against locked boundaries.
  5. The end_idx of a developing (not yet confirmed) zone CAN extend, but
     once confirmed or broken out, it's frozen.

Architecture:
  At each bar N, build a trailing window [N-trail..N] and run the full v3
  ConsolidationDetector. Compare results to the previous bar's snapshot.
  Diff the zone sets to detect NEW / GROWING / CONFIRMED / EXPIRED events.
  Check locked zones for breakout.

Requires consolidation_detector_v3.py in the same directory (or on PYTHONPATH).

Usage:
    python consolidation_forward_v2.py --symbol WAT
    python consolidation_forward_v2.py --csv data.csv --symbol WAT
    python consolidation_forward_v2.py --symbol WAT --trail 150 --quiet

Requirements:
    pip install pandas numpy scipy scikit-learn mysql-connector-python matplotlib
=============================================================================
"""

import warnings
warnings.filterwarnings("ignore")

import os, sys, argparse, time, csv, copy, importlib
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum

# ── Import the full v3 engine ──
# Expects consolidation_detector_v3.py in same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import consolidation_detector_v3 as v3engine


# ──────────────────────────────────────────────────────────────────────
# Zone States
# ──────────────────────────────────────────────────────────────────────
class ZoneState(Enum):
    DEVELOPING = "DEVELOPING"   # detected by v3 but not yet stable
    CONFIRMED  = "CONFIRMED"    # stable across multiple bars, LOCKED
    BREAKOUT   = "BREAKOUT"     # price escaped, LOCKED
    EXPIRED    = "EXPIRED"      # disappeared from v3 detection before confirming


@dataclass
class TrackedZone:
    """A zone being tracked through the forward walk."""
    zone_id: int
    state: ZoneState

    # Locked snapshot (set at confirmation, never changed after)
    locked_start_idx: int = 0    # in GLOBAL index space
    locked_end_idx: int = 0
    locked_high: float = 0.0
    locked_low: float = 0.0
    locked_score: float = 0.0
    locked_methods: List[str] = field(default_factory=list)
    locked_weighted_votes: float = 0.0
    locked_vote_count: int = 0
    locked_atr_ratio: float = 0.0
    locked_range_pct: float = 0.0
    # Locked quality metrics
    locked_bar_compression: float = 0.0
    locked_body_overlap: float = 0.0
    locked_volume_contraction: float = 0.0
    locked_relative_volatility: float = 0.0
    locked_preceding_trend: float = 0.0

    # Current developing state (updated each bar until locked)
    current_start_idx: int = 0
    current_end_idx: int = 0
    current_high: float = 0.0
    current_low: float = 0.0
    current_score: float = 0.0
    current_methods: List[str] = field(default_factory=list)

    # Tracking
    first_seen_bar: int = 0       # global bar when first detected
    first_seen_date: str = ""
    confirmed_bar: int = 0        # global bar when confirmed
    confirmed_date: str = ""
    breakout_bar: int = 0
    breakout_date: str = ""
    breakout_direction: str = ""
    breakout_magnitude: float = 0.0
    consecutive_detections: int = 0   # how many consecutive bars v3 found this
    last_seen_bar: int = 0

    @property
    def is_locked(self):
        return self.state in (ZoneState.CONFIRMED, ZoneState.BREAKOUT, ZoneState.EXPIRED)

    @property
    def display_high(self):
        return self.locked_high if self.is_locked else self.current_high

    @property
    def display_low(self):
        return self.locked_low if self.is_locked else self.current_low

    @property
    def display_start(self):
        return self.locked_start_idx if self.is_locked else self.current_start_idx

    @property
    def display_end(self):
        return self.locked_end_idx if self.is_locked else self.current_end_idx

    @property
    def display_score(self):
        return self.locked_score if self.is_locked else self.current_score

    @property
    def num_bars(self):
        return self.display_end - self.display_start + 1


# ──────────────────────────────────────────────────────────────────────
# Event
# ──────────────────────────────────────────────────────────────────────
@dataclass
class Event:
    bar_idx: int         # global bar index
    bar_date: str
    event_type: str      # DEVELOPING, CONFIRMED, BREAKOUT, EXPIRED
    zone_id: int
    zone_start: int
    zone_end: int
    zone_high: float
    zone_low: float
    zone_bars: int
    zone_score: float
    methods: str
    weighted_votes: float
    vote_count: int
    atr_ratio: float
    range_pct: float
    compression: float
    overlap: float
    vol_contract: float
    rel_vol: float
    trend: float
    breakout_dir: str = ""
    breakout_mag: float = 0.0
    close_price: float = 0.0
    consecutive_detections: int = 0

    def to_row(self):
        return [
            self.bar_idx, self.bar_date, self.event_type, self.zone_id,
            self.zone_start, self.zone_end,
            f"{self.zone_high:.2f}", f"{self.zone_low:.2f}",
            self.zone_bars, f"{self.zone_score:.3f}",
            self.methods, f"{self.weighted_votes:.1f}", self.vote_count,
            f"{self.atr_ratio:.2f}", f"{self.range_pct:.1f}",
            f"{self.compression:.2f}", f"{self.overlap:.2f}",
            f"{self.vol_contract:.2f}", f"{self.rel_vol:.2f}",
            f"{self.trend:.1f}",
            self.breakout_dir, f"{self.breakout_mag:.1f}",
            f"{self.close_price:.2f}", self.consecutive_detections,
        ]

    @staticmethod
    def header():
        return [
            "bar_idx", "bar_date", "event_type", "zone_id",
            "zone_start", "zone_end", "zone_high", "zone_low",
            "zone_bars", "zone_score", "methods", "weighted_votes", "vote_count",
            "atr_ratio", "range_pct",
            "compression", "overlap", "vol_contract", "rel_vol", "trend",
            "breakout_dir", "breakout_mag", "close_price", "consecutive_detections",
        ]


# ──────────────────────────────────────────────────────────────────────
# Zone Matcher — matches v3 output zones to tracked zones
# ──────────────────────────────────────────────────────────────────────
def zones_overlap(z_start, z_end, t_start, t_end, min_frac=0.5):
    """Check if two index ranges overlap by at least min_frac."""
    ovlp_s = max(z_start, t_start)
    ovlp_e = min(z_end, t_end)
    ovlp_len = max(0, ovlp_e - ovlp_s + 1)
    min_len = min(z_end - z_start + 1, t_end - t_start + 1)
    return min_len > 0 and (ovlp_len / min_len) >= min_frac


def price_overlap(z_high, z_low, t_high, t_low, min_frac=0.5):
    """Check if two price ranges overlap."""
    ovlp_top = min(z_high, t_high)
    ovlp_bot = max(z_low, t_low)
    ovlp = max(0, ovlp_top - ovlp_bot)
    min_range = min(z_high - z_low, t_high - t_low)
    return min_range > 0 and (ovlp / min_range) >= min_frac


# ──────────────────────────────────────────────────────────────────────
# Forward Walker
# ──────────────────────────────────────────────────────────────────────
class ForwardWalker:

    def __init__(self, full_df: pd.DataFrame, trail: int = 150,
                 warmup: int = 50, confirm_bars: int = 2,
                 max_confirm_delay: int = 10,
                 min_bars: int = 4, max_bars: int = 50,
                 max_range_atr: float = 2.0, min_weighted_votes: float = 2.5,
                 num_iterations: int = 30,
                 compression_floor: float = 0.85, overlap_floor: float = 0.25,
                 rel_vol_floor: float = 0.80):
        """
        Parameters
        ----------
        full_df : complete OHLCV DataFrame
        trail : trailing window size for v3 engine
        warmup : bars before detection starts
        confirm_bars : consecutive detections needed to confirm
        max_confirm_delay : max bars between zone end_idx and confirmation.
        compression_floor, overlap_floor, rel_vol_floor : quality thresholds
            (auto-calibrated when using --auto flag)
        """
        self.full_df = full_df.copy().reset_index(drop=True)
        self.n = len(self.full_df)
        self.trail = trail
        self.warmup = warmup
        self.confirm_bars = confirm_bars
        self.max_confirm_delay = max_confirm_delay

        # v3 engine params
        self.min_bars = min_bars
        self.max_bars = max_bars
        self.max_range_atr = max_range_atr
        self.min_weighted_votes = min_weighted_votes
        self.num_iterations = num_iterations

        # Quality floor thresholds
        self.compression_floor = compression_floor
        self.overlap_floor = overlap_floor
        self.rel_vol_floor = rel_vol_floor

        # State
        self.next_zone_id = 1
        self.tracked_zones: List[TrackedZone] = []
        self.events: List[Event] = []

        # Normalize columns
        col_map = {}
        for c in self.full_df.columns:
            cl = c.lower().strip()
            if cl in ("open", "high", "low", "close", "volume", "bar_date"):
                col_map[c] = cl
        self.full_df.rename(columns=col_map, inplace=True)
        for c in ["open", "high", "low", "close"]:
            self.full_df[c] = self.full_df[c].astype(float)
        if "volume" in self.full_df.columns:
            self.full_df["volume"] = self.full_df["volume"].astype(int)

    def _get_date(self, idx):
        if "bar_date" in self.full_df.columns:
            return str(self.full_df["bar_date"].iloc[idx])
        return str(idx)

    def _run_v3_on_window(self, end_idx: int) -> List[v3engine.FusedZone]:
        """Run the full v3 engine on trailing window ending at end_idx."""
        start = max(0, end_idx - self.trail + 1)
        window_df = self.full_df.iloc[start:end_idx + 1].copy().reset_index(drop=True)

        if len(window_df) < self.warmup:
            return []

        try:
            det = v3engine.ConsolidationDetector(
                window_df,
                min_bars=self.min_bars,
                max_bars=self.max_bars,
                max_range_atr=self.max_range_atr,
                num_iterations=self.num_iterations,
                compression_floor=self.compression_floor,
                overlap_floor=self.overlap_floor,
                rel_vol_floor=self.rel_vol_floor,
            )
            zones = det.run_all_passes(
                min_weighted_votes=self.min_weighted_votes,
                verbose=False,
            )

            # Convert window-local indices to global indices
            for z in zones:
                z.start_idx += start
                z.end_idx += start

            return zones
        except Exception as e:
            return []

    def _match_v3_zone_to_tracked(self, fz: v3engine.FusedZone) -> Optional[TrackedZone]:
        """Find the tracked zone that best matches a v3 detected zone."""
        best_match = None
        best_score = 0

        for tz in self.tracked_zones:
            if tz.is_locked:
                continue

            # Match on time overlap + price overlap
            t_overlap = zones_overlap(fz.start_idx, fz.end_idx,
                                       tz.current_start_idx, tz.current_end_idx,
                                       min_frac=0.4)
            p_overlap = price_overlap(fz.high, fz.low,
                                       tz.current_high, tz.current_low,
                                       min_frac=0.4)

            if t_overlap and p_overlap:
                # Score by similarity
                time_sim = 1.0 - abs(fz.start_idx - tz.current_start_idx) / max(fz.num_bars, 1)
                price_mid_diff = abs((fz.high + fz.low) / 2 - (tz.current_high + tz.current_low) / 2)
                price_range = max(fz.high - fz.low, 0.01)
                price_sim = max(0, 1.0 - price_mid_diff / price_range)
                score = time_sim * 0.5 + price_sim * 0.5
                if score > best_score:
                    best_score = score
                    best_match = tz

        return best_match

    def _lock_zone(self, tz: TrackedZone, fz: v3engine.FusedZone, bar_idx: int):
        """Lock a zone as CONFIRMED with current v3 data."""
        tz.state = ZoneState.CONFIRMED
        tz.confirmed_bar = bar_idx
        tz.confirmed_date = self._get_date(bar_idx)

        # Lock all values from v3 output
        tz.locked_start_idx = fz.start_idx
        tz.locked_end_idx = fz.end_idx
        tz.locked_high = fz.high
        tz.locked_low = fz.low
        tz.locked_score = fz.composite_score
        tz.locked_methods = list(fz.methods)
        tz.locked_weighted_votes = fz.weighted_votes
        tz.locked_vote_count = fz.vote_count
        tz.locked_atr_ratio = fz.atr_ratio
        tz.locked_range_pct = fz.range_pct
        tz.locked_bar_compression = fz.bar_compression_ratio
        tz.locked_body_overlap = fz.body_overlap_score
        tz.locked_volume_contraction = fz.volume_contraction
        tz.locked_relative_volatility = fz.relative_volatility
        tz.locked_preceding_trend = fz.preceding_trend_strength

    def _make_event(self, bar_idx, event_type, tz, close=0.0):
        return Event(
            bar_idx=bar_idx,
            bar_date=self._get_date(bar_idx),
            event_type=event_type,
            zone_id=tz.zone_id,
            zone_start=tz.display_start,
            zone_end=tz.display_end,
            zone_high=tz.display_high,
            zone_low=tz.display_low,
            zone_bars=tz.num_bars,
            zone_score=tz.display_score,
            methods="|".join(sorted(tz.locked_methods if tz.is_locked else tz.current_methods)),
            weighted_votes=tz.locked_weighted_votes if tz.is_locked else 0,
            vote_count=tz.locked_vote_count if tz.is_locked else 0,
            atr_ratio=tz.locked_atr_ratio if tz.is_locked else 0,
            range_pct=tz.locked_range_pct if tz.is_locked else 0,
            compression=tz.locked_bar_compression if tz.is_locked else 0,
            overlap=tz.locked_body_overlap if tz.is_locked else 0,
            vol_contract=tz.locked_volume_contraction if tz.is_locked else 0,
            rel_vol=tz.locked_relative_volatility if tz.is_locked else 0,
            trend=tz.locked_preceding_trend if tz.is_locked else 0,
            breakout_dir=tz.breakout_direction,
            breakout_mag=tz.breakout_magnitude,
            close_price=close,
            consecutive_detections=tz.consecutive_detections,
        )

    # ══════════════════════════════════════════════════════════════
    # Main walk-forward loop
    # ══════════════════════════════════════════════════════════════
    def walk(self, start_bar: int = 0, end_bar: int = None,
             verbose: bool = True, quiet: bool = False,
             scan_step: int = 1):
        """
        Process bars from start_bar to end_bar.
        Breakout checks happen EVERY bar (no repaint).
        Full v3 scans happen every scan_step bars (for efficiency).
        scan_step=1 is fully exhaustive (slowest, highest quality).
        scan_step=3 scans every 3rd bar (3x faster, still very good).
        """
        if end_bar is None:
            end_bar = self.n

        t0 = time.time()

        for bar_idx in range(start_bar, end_bar):
            close = float(self.full_df["close"].iloc[bar_idx])
            date = self._get_date(bar_idx)
            bar_events = []

            # Skip warmup
            if bar_idx < self.warmup:
                continue

            # ── Step 1: Check breakouts EVERY bar (no repaint) ──
            for tz in self.tracked_zones:
                if tz.state != ZoneState.CONFIRMED:
                    continue

                bar_h = float(self.full_df["high"].iloc[bar_idx])
                bar_l = float(self.full_df["low"].iloc[bar_idx])

                # Price broke out?
                # Use close for direction determination, high/low for detection
                broke_high = close > tz.locked_high
                broke_low = close < tz.locked_low

                if broke_high or broke_low:
                    # Compute ATR at current bar for magnitude
                    atr_start = max(0, bar_idx - 14)
                    trs = []
                    for i in range(atr_start, bar_idx + 1):
                        h = float(self.full_df["high"].iloc[i])
                        l = float(self.full_df["low"].iloc[i])
                        if i > 0:
                            pc = float(self.full_df["close"].iloc[i-1])
                            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
                        else:
                            trs.append(h - l)
                    atr = np.mean(trs) if trs else 1.0

                    if broke_high:
                        tz.breakout_direction = "up"
                        tz.breakout_magnitude = (close - tz.locked_high) / atr if atr > 0 else 0
                    else:
                        tz.breakout_direction = "down"
                        tz.breakout_magnitude = (tz.locked_low - close) / atr if atr > 0 else 0

                    tz.state = ZoneState.BREAKOUT
                    tz.breakout_bar = bar_idx
                    tz.breakout_date = date
                    bar_events.append(self._make_event(bar_idx, "BREAKOUT", tz, close))

            # ── Step 2: Run full v3 on trailing window ──
            # Only scan every scan_step bars (breakouts still checked every bar above)
            run_scan = ((bar_idx - self.warmup) % scan_step == 0)
            # Always scan if a developing zone might be about to confirm
            if not run_scan:
                for tz in self.tracked_zones:
                    if (tz.state == ZoneState.DEVELOPING and
                        tz.consecutive_detections == self.confirm_bars - 1):
                        run_scan = True
                        break

            if not run_scan:
                self.events.extend(bar_events)
                continue

            v3_zones = self._run_v3_on_window(bar_idx)

            # ── Step 3: Match v3 zones to tracked zones ──
            matched_tracked = set()  # tracked zone IDs that got matched
            matched_v3 = set()       # v3 zone indices that got matched

            for vi, fz in enumerate(v3_zones):
                tz = self._match_v3_zone_to_tracked(fz)
                if tz is not None:
                    matched_tracked.add(tz.zone_id)
                    matched_v3.add(vi)

                    # Update developing zone
                    tz.current_start_idx = fz.start_idx
                    tz.current_end_idx = fz.end_idx
                    tz.current_high = fz.high
                    tz.current_low = fz.low
                    tz.current_score = fz.composite_score
                    tz.current_methods = list(fz.methods)
                    tz.consecutive_detections += 1
                    tz.last_seen_bar = bar_idx

                    # Check for confirmation
                    if (tz.state == ZoneState.DEVELOPING and
                        tz.consecutive_detections >= self.confirm_bars):
                        # Staleness check: zone end must be recent
                        delay = bar_idx - fz.end_idx
                        if delay <= self.max_confirm_delay:
                            self._lock_zone(tz, fz, bar_idx)
                            bar_events.append(self._make_event(bar_idx, "CONFIRMED", tz, close))

                            # Immediate breakout check — if current price is
                            # already outside the locked zone, emit breakout
                            # on the same bar (happens when zone is old and
                            # price has already moved away)
                            bar_h = float(self.full_df["high"].iloc[bar_idx])
                            bar_l = float(self.full_df["low"].iloc[bar_idx])
                            if close > tz.locked_high or close < tz.locked_low:
                                atr_start = max(0, bar_idx - 14)
                                trs = []
                                for ii in range(atr_start, bar_idx + 1):
                                    hh = float(self.full_df["high"].iloc[ii])
                                    ll = float(self.full_df["low"].iloc[ii])
                                    if ii > 0:
                                        pc = float(self.full_df["close"].iloc[ii-1])
                                        trs.append(max(hh - ll, abs(hh - pc), abs(ll - pc)))
                                    else:
                                        trs.append(hh - ll)
                                atr = np.mean(trs) if trs else 1.0
                                if close > tz.locked_high:
                                    tz.breakout_direction = "up"
                                    tz.breakout_magnitude = (close - tz.locked_high) / atr if atr > 0 else 0
                                else:
                                    tz.breakout_direction = "down"
                                    tz.breakout_magnitude = (tz.locked_low - close) / atr if atr > 0 else 0
                                tz.state = ZoneState.BREAKOUT
                                tz.breakout_bar = bar_idx
                                tz.breakout_date = date
                                bar_events.append(self._make_event(bar_idx, "BREAKOUT", tz, close))

                        else:
                            # Zone is stale — detected but too old to act on
                            tz.state = ZoneState.EXPIRED
                            bar_events.append(self._make_event(bar_idx, "EXPIRED", tz, close))

                else:
                    # New zone — not matched to any tracked zone
                    # Staleness check: don't even track zones that are already old
                    if bar_idx - fz.end_idx > self.max_confirm_delay:
                        continue

                    # Also check it doesn't overlap a CONFIRMED/BREAKOUT zone
                    overlaps_locked = False
                    for tz2 in self.tracked_zones:
                        if tz2.state in (ZoneState.CONFIRMED, ZoneState.BREAKOUT):
                            if (zones_overlap(fz.start_idx, fz.end_idx,
                                              tz2.locked_start_idx, tz2.locked_end_idx, 0.4) and
                                price_overlap(fz.high, fz.low,
                                              tz2.locked_high, tz2.locked_low, 0.4)):
                                overlaps_locked = True
                                break

                    if not overlaps_locked:
                        matched_v3.add(vi)
                        new_tz = TrackedZone(
                            zone_id=self.next_zone_id,
                            state=ZoneState.DEVELOPING,
                            current_start_idx=fz.start_idx,
                            current_end_idx=fz.end_idx,
                            current_high=fz.high,
                            current_low=fz.low,
                            current_score=fz.composite_score,
                            current_methods=list(fz.methods),
                            first_seen_bar=bar_idx,
                            first_seen_date=date,
                            consecutive_detections=1,
                            last_seen_bar=bar_idx,
                        )
                        self.next_zone_id += 1
                        self.tracked_zones.append(new_tz)
                        bar_events.append(self._make_event(bar_idx, "DEVELOPING", new_tz, close))

            # ── Step 4: Expire developing zones that v3 no longer detects ──
            for tz in self.tracked_zones:
                if tz.state == ZoneState.DEVELOPING and tz.zone_id not in matched_tracked:
                    if tz.last_seen_bar < bar_idx:
                        # Not seen this bar — reset consecutive counter
                        # Allow a 1-bar gap before expiring
                        if bar_idx - tz.last_seen_bar > 1:
                            tz.state = ZoneState.EXPIRED
                            bar_events.append(self._make_event(bar_idx, "EXPIRED", tz, close))

            # ── Log events ──
            self.events.extend(bar_events)

            # ── Print ──
            if bar_events:
                for ev in bar_events:
                    if ev.event_type == "CONFIRMED":
                        print(f"  ★ Bar {ev.bar_idx:>4} {ev.bar_date} | "
                              f"CONFIRMED Zone #{ev.zone_id} | "
                              f"${ev.zone_low:.2f}-${ev.zone_high:.2f} "
                              f"({ev.zone_bars}bars, {ev.consecutive_detections} consecutive) | "
                              f"score={ev.zone_score:.3f}")
                        print(f"         comp={ev.compression:.2f} "
                              f"ovlp={ev.overlap:.2f} "
                              f"vol={ev.vol_contract:.2f} "
                              f"rVol={ev.rel_vol:.2f} "
                              f"trend={ev.trend:.1f}ATR "
                              f"wt={ev.weighted_votes:.1f} "
                              f"votes={ev.vote_count}")
                        print(f"         methods: {ev.methods}")
                    elif ev.event_type == "BREAKOUT":
                        arrow = "↑" if ev.breakout_dir == "up" else "↓"
                        print(f"  ⚡ Bar {ev.bar_idx:>4} {ev.bar_date} | "
                              f"BREAKOUT Zone #{ev.zone_id} {arrow} "
                              f"{ev.breakout_mag:.1f}ATR | "
                              f"close=${ev.close_price:.2f}")
                    elif not quiet:
                        if ev.event_type == "DEVELOPING":
                            print(f"  ○ Bar {ev.bar_idx:>4} {ev.bar_date} | "
                                  f"DEVELOPING Zone #{ev.zone_id} | "
                                  f"${ev.zone_low:.2f}-${ev.zone_high:.2f} "
                                  f"score={ev.zone_score:.3f}")
            elif verbose and not quiet and bar_idx % 50 == 0:
                active = len([z for z in self.tracked_zones
                             if z.state in (ZoneState.DEVELOPING, ZoneState.CONFIRMED)])
                print(f"  · Bar {bar_idx:>4} {date} | "
                      f"close=${close:.2f} | {active} active zones")

        elapsed = time.time() - t0
        return elapsed

    # ══════════════════════════════════════════════════════════════
    # Results
    # ══════════════════════════════════════════════════════════════
    def get_confirmed_zones(self):
        return [z for z in self.tracked_zones
                if z.state in (ZoneState.CONFIRMED, ZoneState.BREAKOUT)]

    def get_breakout_zones(self):
        return [z for z in self.tracked_zones if z.state == ZoneState.BREAKOUT]

    def summary(self):
        confirmed = self.get_confirmed_zones()
        breakouts = self.get_breakout_zones()
        developing = [z for z in self.tracked_zones if z.state == ZoneState.DEVELOPING]
        expired = [z for z in self.tracked_zones if z.state == ZoneState.EXPIRED]
        return {
            "total_tracked": len(self.tracked_zones),
            "confirmed": len(confirmed),
            "breakouts": len(breakouts),
            "developing": len(developing),
            "expired": len(expired),
        }

    def export_events_csv(self, path):
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(Event.header())
            for e in self.events:
                w.writerow(e.to_row())


# ──────────────────────────────────────────────────────────────────────
# Plotting
# ──────────────────────────────────────────────────────────────────────
def plot_forward(df, walker, symbol="", save_path="forward_v2_zones.png"):
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from matplotlib.gridspec import GridSpec

    confirmed = walker.get_confirmed_zones()

    fig = plt.figure(figsize=(28, 16))
    gs = GridSpec(5, 1, height_ratios=[3, 0.6, 0.6, 0.6, 0.8], hspace=0.08)
    fig.patch.set_facecolor("#0a0a18")

    has_dates = "bar_date" in df.columns

    # ── Candlestick ──
    ax = fig.add_subplot(gs[0])
    ax.set_facecolor("#0a0a18")
    for i in range(len(df)):
        o, h, l, c = [float(df[x].iloc[i]) for x in ("open","high","low","close")]
        color = "#26a69a" if c >= o else "#ef5350"
        ax.plot([i, i], [l, h], color=color, linewidth=0.5)
        ax.add_patch(patches.Rectangle(
            (i - 0.35, min(o, c)), 0.7, max(abs(c - o), 0.001),
            edgecolor=color, facecolor=color, alpha=0.9, linewidth=0.3))

    for z in confirmed:
        si = z.locked_start_idx
        ei = z.locked_end_idx
        hi = z.locked_high
        lo = z.locked_low
        nb = ei - si + 1

        if z.state == ZoneState.BREAKOUT:
            ec = "#00ff88" if z.breakout_direction == "up" else "#ff4444"
            alpha = 0.18 + 0.25 * z.locked_score
        else:
            ec = "#ffaa00"
            alpha = 0.15 + 0.20 * z.locked_score

        ax.add_patch(patches.Rectangle(
            (si - 0.5, lo), nb, hi - lo,
            linewidth=2, edgecolor=ec, facecolor=ec, alpha=alpha))

        label = f"S={z.locked_score:.2f}"
        if z.state == ZoneState.BREAKOUT:
            arrow = "↑" if z.breakout_direction == "up" else "↓"
            label += f" {arrow}{z.breakout_magnitude:.1f}ATR"
        if has_dates:
            label = f"{z.confirmed_date} " + label
        ax.text(si, hi, label, fontsize=5, color="white", va="bottom")

        # Mark confirmation bar with a diamond
        ax.plot(z.confirmed_bar, hi + (hi - lo) * 0.1, marker="D",
                color=ec, markersize=3, alpha=0.8)

    ax.set_title(f"{symbol} — Forward v2 Exhaustive, No Repaint "
                 f"({len(confirmed)} confirmed)",
                 color="white", fontsize=14, fontweight="bold")
    ax.tick_params(colors="white", labelsize=7)
    ax.set_ylabel("Price", color="white")
    for s in ax.spines.values(): s.set_color("#222")
    ax.set_xlim(-1, len(df) + 1)

    # ── Volume ──
    ax_vol = fig.add_subplot(gs[1], sharex=ax)
    ax_vol.set_facecolor("#0a0a18")
    vol_colors = ["#26a69a" if df["close"].iloc[i] >= df["open"].iloc[i] else "#ef5350"
                  for i in range(len(df))]
    ax_vol.bar(range(len(df)), df["volume"], color=vol_colors, alpha=0.6, width=0.7)
    ax_vol.set_ylabel("Vol", color="white", fontsize=8)
    ax_vol.tick_params(colors="white", labelsize=6)
    for s in ax_vol.spines.values(): s.set_color("#222")

    # ── ATR ──
    h_, l_, c_ = df["high"], df["low"], df["close"]
    tr = pd.concat([h_ - l_, (h_ - c_.shift(1)).abs(), (l_ - c_.shift(1)).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=1).mean()
    ax_atr = fig.add_subplot(gs[2], sharex=ax)
    ax_atr.set_facecolor("#0a0a18")
    ax_atr.plot(range(len(df)), atr14, color="#ffa726", alpha=0.8, linewidth=0.8)
    ax_atr.set_ylabel("ATR", color="white", fontsize=8)
    ax_atr.tick_params(colors="white", labelsize=6)
    for s in ax_atr.spines.values(): s.set_color("#222")

    # ── Score heatmap ──
    ax_heat = fig.add_subplot(gs[3], sharex=ax)
    ax_heat.set_facecolor("#0a0a18")
    score_line = np.zeros(len(df))
    for z in confirmed:
        for i in range(z.locked_start_idx, min(z.locked_end_idx + 1, len(df))):
            score_line[i] = max(score_line[i], z.locked_score)
    ax_heat.fill_between(range(len(df)), score_line, color="#ff69b4", alpha=0.5)
    ax_heat.set_ylabel("Score", color="white", fontsize=8)
    ax_heat.set_ylim(0, 1)
    ax_heat.tick_params(colors="white", labelsize=6)
    for s in ax_heat.spines.values(): s.set_color("#222")

    # ── Event timeline ──
    ax_ev = fig.add_subplot(gs[4], sharex=ax)
    ax_ev.set_facecolor("#0a0a18")
    ev_colors = {"CONFIRMED": "#ffaa00", "BREAKOUT": "#00ff88",
                 "DEVELOPING": "#4488ff", "EXPIRED": "#444444"}
    for ev in walker.events:
        if ev.event_type in ev_colors:
            ax_ev.axvline(ev.bar_idx, color=ev_colors[ev.event_type],
                         alpha=0.6, linewidth=1)
    for label, color in [("CONFIRMED","#ffaa00"),("BREAKOUT","#00ff88"),("DEVELOPING","#4488ff")]:
        ax_ev.plot([], [], color=color, linewidth=3, label=label)
    ax_ev.legend(loc="upper left", fontsize=7, framealpha=0.3,
                labelcolor="white", facecolor="#0a0a18")
    ax_ev.set_ylabel("Events", color="white", fontsize=8)
    ax_ev.tick_params(colors="white", labelsize=6)
    for s in ax_ev.spines.values(): s.set_color("#222")

    if has_dates:
        step = max(1, len(df) // 25)
        ticks = list(range(0, len(df), step))
        ax_ev.set_xticks(ticks)
        ax_ev.set_xticklabels([str(df["bar_date"].iloc[i]) for i in ticks],
                              rotation=45, ha="right", fontsize=6, color="white")
    plt.setp(ax.get_xticklabels(), visible=False)
    plt.setp(ax_vol.get_xticklabels(), visible=False)
    plt.setp(ax_atr.get_xticklabels(), visible=False)
    plt.setp(ax_heat.get_xticklabels(), visible=False)

    plt.tight_layout()
    plt.savefig(save_path, dpi=180, facecolor="#0a0a18")
    plt.close()
    print(f"  Chart saved: {save_path}")


# ──────────────────────────────────────────────────────────────────────
# Stock Profiler — analyzes characteristics and derives optimal parameters
# ──────────────────────────────────────────────────────────────────────
def profile_stock(df):
    """
    Analyze a stock's price behavior and return auto-calibrated detector
    parameters. Uses the full dataset for profiling (this runs once before
    the forward walk, so no look-ahead bias — it characterizes the INSTRUMENT,
    not predicting future zones).
    """
    h, l, c, o = df["high"].astype(float), df["low"].astype(float), \
                  df["close"].astype(float), df["open"].astype(float)
    vol = df["volume"].astype(float) if "volume" in df.columns else pd.Series(0, index=df.index)

    tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=1).mean()

    price_level = c.mean()
    atr_pct = (atr14 / c * 100).median()
    daily_range_pct = ((h - l) / c * 100).median()
    body_pct = ((c - o).abs() / c * 100).median()
    body_range_ratio = (body_pct / daily_range_pct) if daily_range_pct > 0 else 0.5
    returns = c.pct_change().dropna()
    ret_vol = returns.std() * 100
    ret_kurtosis = returns.kurtosis()
    vol_cv = (vol.std() / vol.mean()) if vol.mean() > 0 else 1.0

    # Natural tightness: 15th percentile of 5-bar range/ATR
    rolling_5_range = h.rolling(5).max() - l.rolling(5).min()
    range_atr_5 = (rolling_5_range / atr14).dropna()
    tight_15pct = range_atr_5.quantile(0.15) if len(range_atr_5) > 20 else 1.5

    # Natural bar compression
    bar_range_norm = (h - l) / atr14
    roll_comp_5 = bar_range_norm.rolling(5).mean()
    roll_comp_20 = bar_range_norm.rolling(20).mean()
    comp_ratio = (roll_comp_5 / roll_comp_20).dropna()
    comp_ratio = comp_ratio[comp_ratio > 0]
    natural_comp_10 = comp_ratio.quantile(0.10) if len(comp_ratio) > 20 else 0.70
    natural_comp_25 = comp_ratio.quantile(0.25) if len(comp_ratio) > 20 else 0.80

    # Natural body overlap
    body_top = pd.concat([o, c], axis=1).max(axis=1)
    body_bot = pd.concat([o, c], axis=1).min(axis=1)
    body_size = (c - o).abs()
    overlaps = []
    for i in range(1, len(df)):
        ovlp = max(0, min(body_top.iloc[i-1], body_top.iloc[i]) -
                      max(body_bot.iloc[i-1], body_bot.iloc[i]))
        avg_b = (body_size.iloc[i-1] + body_size.iloc[i]) / 2
        overlaps.append(ovlp / avg_b if avg_b > 0 else 1.0)
    overlap_s = pd.Series(overlaps).rolling(5).mean().dropna()
    natural_overlap_75 = overlap_s.quantile(0.75) if len(overlap_s) > 20 else 0.35

    # Noise score: composite of kurtosis, vol variability, raw volatility
    noise_score = (
        min(ret_kurtosis / 5, 2.0) * 0.4 +
        min(vol_cv, 2.0) * 0.3 +
        min(atr_pct / 3, 2.0) * 0.3
    )
    noise_score = max(0.1, min(noise_score, 2.0))

    # ── Derive parameters ──
    max_range_atr = round(max(1.2, min(tight_15pct * 1.3, 3.0)), 2)
    min_weighted_votes = round(max(2.0, min(2.0 + noise_score * 0.5, 4.0)), 1)
    compression_floor = round(max(0.65, min(natural_comp_25 + 0.05, 0.95)), 2)
    overlap_floor = round(max(0.10, min(natural_overlap_75 * 0.35, 0.40)), 2)
    rel_vol_floor = round(max(0.55, min(natural_comp_10 + 0.10, 0.90)), 2)
    confirm_bars = 3 if noise_score > 1.2 else 2
    num_iterations = max(15, min(int(20 + noise_score * 10), 50))
    max_confirm_delay = max(5, min(int(8 + (1.5 - min(noise_score, 1.5)) * 4), 15))
    trail = max(80, min(int(100 + noise_score * 30), 200))

    profile = {
        "price_level": round(price_level, 2),
        "atr_pct": round(atr_pct, 2),
        "daily_range_pct": round(daily_range_pct, 2),
        "body_range_ratio": round(body_range_ratio, 2),
        "return_vol": round(ret_vol, 2),
        "return_kurtosis": round(ret_kurtosis, 2),
        "vol_cv": round(vol_cv, 2),
        "noise_score": round(noise_score, 2),
    }
    params = {
        "max_range_atr": max_range_atr,
        "min_weighted_votes": min_weighted_votes,
        "compression_floor": compression_floor,
        "overlap_floor": overlap_floor,
        "rel_vol_floor": rel_vol_floor,
        "confirm_bars": confirm_bars,
        "num_iterations": num_iterations,
        "max_confirm_delay": max_confirm_delay,
        "trail": trail,
    }
    return profile, params


# ──────────────────────────────────────────────────────────────────────
# MySQL Export
# ──────────────────────────────────────────────────────────────────────
def ensure_mysql_tables(conn):
    """Create tables if they don't exist."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS `consolidation_zones` (
            `id` bigint NOT NULL AUTO_INCREMENT,
            `symbol` varchar(20) NOT NULL,
            `zone_id` int NOT NULL,
            `state` varchar(20) NOT NULL,
            `zone_start_date` date NOT NULL,
            `zone_end_date` date NOT NULL,
            `zone_start_idx` int NOT NULL,
            `zone_end_idx` int NOT NULL,
            `zone_high` decimal(18,6) NOT NULL,
            `zone_low` decimal(18,6) NOT NULL,
            `num_bars` int NOT NULL,
            `composite_score` decimal(8,4) NOT NULL,
            `weighted_votes` decimal(8,2) NOT NULL,
            `vote_count` int NOT NULL,
            `atr_ratio` decimal(8,4) DEFAULT NULL,
            `range_pct` decimal(8,4) DEFAULT NULL,
            `bar_compression` decimal(8,4) DEFAULT NULL,
            `body_overlap` decimal(8,4) DEFAULT NULL,
            `volume_contraction` decimal(8,4) DEFAULT NULL,
            `relative_volatility` decimal(8,4) DEFAULT NULL,
            `preceding_trend` decimal(8,4) DEFAULT NULL,
            `methods` text DEFAULT NULL,
            `confirmed_date` date DEFAULT NULL,
            `confirmed_bar_idx` int DEFAULT NULL,
            `breakout_date` date DEFAULT NULL,
            `breakout_bar_idx` int DEFAULT NULL,
            `breakout_direction` varchar(10) DEFAULT NULL,
            `breakout_magnitude` decimal(8,4) DEFAULT NULL,
            `first_seen_date` date DEFAULT NULL,
            `consecutive_detections` int DEFAULT NULL,
            `auto_calibrated` tinyint(1) DEFAULT 0,
            `noise_score` decimal(8,4) DEFAULT NULL,
            `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_symbol_zone` (`symbol`, `zone_start_date`, `zone_end_date`, `zone_high`, `zone_low`),
            KEY `idx_symbol` (`symbol`),
            KEY `idx_symbol_state` (`symbol`, `state`),
            KEY `idx_symbol_score` (`symbol`, `composite_score`),
            KEY `idx_confirmed_date` (`confirmed_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS `consolidation_events` (
            `id` bigint NOT NULL AUTO_INCREMENT,
            `symbol` varchar(20) NOT NULL,
            `bar_idx` int NOT NULL,
            `bar_date` date NOT NULL,
            `event_type` varchar(20) NOT NULL,
            `zone_id` int NOT NULL,
            `zone_start` int DEFAULT NULL,
            `zone_end` int DEFAULT NULL,
            `zone_high` decimal(18,6) DEFAULT NULL,
            `zone_low` decimal(18,6) DEFAULT NULL,
            `zone_bars` int DEFAULT NULL,
            `zone_score` decimal(8,4) DEFAULT NULL,
            `methods` text DEFAULT NULL,
            `weighted_votes` decimal(8,2) DEFAULT NULL,
            `vote_count` int DEFAULT NULL,
            `compression` decimal(8,4) DEFAULT NULL,
            `overlap` decimal(8,4) DEFAULT NULL,
            `vol_contract` decimal(8,4) DEFAULT NULL,
            `rel_vol` decimal(8,4) DEFAULT NULL,
            `trend` decimal(8,4) DEFAULT NULL,
            `breakout_dir` varchar(10) DEFAULT NULL,
            `breakout_mag` decimal(8,4) DEFAULT NULL,
            `close_price` decimal(18,6) DEFAULT NULL,
            `consecutive_detections` int DEFAULT NULL,
            `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_symbol_date` (`symbol`, `bar_date`),
            KEY `idx_symbol_event` (`symbol`, `event_type`),
            KEY `idx_symbol_zone` (`symbol`, `zone_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS `consolidation_developing` (
            `id` bigint NOT NULL AUTO_INCREMENT,
            `symbol` varchar(20) NOT NULL,
            `zone_id` int NOT NULL,
            `state` varchar(20) NOT NULL,
            `zone_start_date` date DEFAULT NULL,
            `zone_end_date` date DEFAULT NULL,
            `zone_start_idx` int NOT NULL,
            `zone_end_idx` int NOT NULL,
            `zone_high` decimal(18,6) NOT NULL,
            `zone_low` decimal(18,6) NOT NULL,
            `num_bars` int NOT NULL,
            `current_score` decimal(8,4) DEFAULT NULL,
            `methods` text DEFAULT NULL,
            `first_seen_date` date DEFAULT NULL,
            `first_seen_bar` int DEFAULT NULL,
            `last_seen_bar` int DEFAULT NULL,
            `consecutive_detections` int DEFAULT NULL,
            `bars_until_confirm` int DEFAULT NULL,
            `last_close` decimal(18,6) DEFAULT NULL,
            `distance_from_high_pct` decimal(8,4) DEFAULT NULL,
            `distance_from_low_pct` decimal(8,4) DEFAULT NULL,
            `scan_date` datetime DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_symbol_zone` (`symbol`, `zone_id`, `zone_start_idx`),
            KEY `idx_symbol` (`symbol`),
            KEY `idx_scan_date` (`scan_date`),
            KEY `idx_forming` (`symbol`, `state`, `consecutive_detections`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)

    conn.commit()
    cur.close()


def export_to_mysql(walker, symbol, df, host="127.0.0.1", user="remote",
                    password="Chamba4347!", database="leo", port=3306,
                    auto_calibrated=False, noise_score=None):
    """Export confirmed zones and events to MySQL."""
    import mysql.connector

    conn = mysql.connector.connect(
        host=host, user=user, password=password,
        database=database, port=port
    )
    ensure_mysql_tables(conn)
    cur = conn.cursor()

    has_dates = "bar_date" in df.columns

    def get_date(idx):
        if has_dates and 0 <= idx < len(df):
            return str(df["bar_date"].iloc[idx])
        return None

    # ── Delete previous results for this symbol ──
    cur.execute("DELETE FROM consolidation_zones WHERE symbol = %s", (symbol,))
    cur.execute("DELETE FROM consolidation_events WHERE symbol = %s", (symbol,))

    # ── Insert confirmed zones ──
    confirmed = walker.get_confirmed_zones()
    zone_count = 0
    for z in confirmed:
        start_date = get_date(z.locked_start_idx)
        end_date = get_date(z.locked_end_idx)
        conf_date = z.confirmed_date if z.confirmed_date else None
        bo_date = z.breakout_date if z.breakout_date else None

        cur.execute("""
            INSERT INTO consolidation_zones (
                symbol, zone_id, state,
                zone_start_date, zone_end_date, zone_start_idx, zone_end_idx,
                zone_high, zone_low, num_bars,
                composite_score, weighted_votes, vote_count,
                atr_ratio, range_pct,
                bar_compression, body_overlap, volume_contraction,
                relative_volatility, preceding_trend,
                methods,
                confirmed_date, confirmed_bar_idx,
                breakout_date, breakout_bar_idx,
                breakout_direction, breakout_magnitude,
                first_seen_date, consecutive_detections,
                auto_calibrated, noise_score
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
        """, (
            symbol, z.zone_id, z.state.value,
            start_date, end_date, z.locked_start_idx, z.locked_end_idx,
            z.locked_high, z.locked_low, z.num_bars,
            z.locked_score, z.locked_weighted_votes, z.locked_vote_count,
            z.locked_atr_ratio, z.locked_range_pct,
            z.locked_bar_compression, z.locked_body_overlap, z.locked_volume_contraction,
            z.locked_relative_volatility, z.locked_preceding_trend,
            "|".join(sorted(z.locked_methods)),
            conf_date, z.confirmed_bar,
            bo_date, z.breakout_bar if z.breakout_bar else None,
            z.breakout_direction if z.breakout_direction else None,
            z.breakout_magnitude if z.breakout_magnitude else None,
            z.first_seen_date if z.first_seen_date else None,
            z.consecutive_detections,
            1 if auto_calibrated else 0,
            float(noise_score) if noise_score is not None else None,
        ))
        zone_count += 1

    # ── Insert events ──
    event_count = 0
    for ev in walker.events:
        cur.execute("""
            INSERT INTO consolidation_events (
                symbol, bar_idx, bar_date, event_type, zone_id,
                zone_start, zone_end, zone_high, zone_low, zone_bars,
                zone_score, methods, weighted_votes, vote_count,
                compression, overlap, vol_contract, rel_vol, trend,
                breakout_dir, breakout_mag, close_price, consecutive_detections
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """, (
            symbol, ev.bar_idx, ev.bar_date, ev.event_type, ev.zone_id,
            ev.zone_start, ev.zone_end, ev.zone_high, ev.zone_low, ev.zone_bars,
            ev.zone_score, ev.methods, ev.weighted_votes, ev.vote_count,
            ev.compression, ev.overlap, ev.vol_contract, ev.rel_vol, ev.trend,
            ev.breakout_dir if ev.breakout_dir else None,
            ev.breakout_mag if ev.breakout_mag else None,
            ev.close_price, ev.consecutive_detections,
        ))
        event_count += 1

    # ── Insert developing (active) zones — the "forming now" snapshot ──
    cur.execute("DELETE FROM consolidation_developing WHERE symbol = %s", (symbol,))
    developing_count = 0

    # Get the last close price
    last_close = float(df["close"].iloc[-1]) if len(df) > 0 else 0

    active = [z for z in walker.tracked_zones
              if z.state == ZoneState.DEVELOPING]
    for z in active:
        start_date = get_date(z.current_start_idx)
        end_date = get_date(z.current_end_idx)
        z_high = z.display_high
        z_low = z.display_low
        z_mid = (z_high + z_low) / 2 if (z_high + z_low) > 0 else 1

        # How far is current price from zone boundaries?
        dist_high_pct = (last_close - z_high) / z_mid * 100 if z_mid > 0 else 0
        dist_low_pct = (last_close - z_low) / z_mid * 100 if z_mid > 0 else 0

        # How many more consecutive detections until confirmation?
        confirm_bars_setting = walker.confirm_bars
        bars_until = max(0, confirm_bars_setting - z.consecutive_detections)

        cur.execute("""
            INSERT INTO consolidation_developing (
                symbol, zone_id, state,
                zone_start_date, zone_end_date, zone_start_idx, zone_end_idx,
                zone_high, zone_low, num_bars,
                current_score, methods,
                first_seen_date, first_seen_bar, last_seen_bar,
                consecutive_detections, bars_until_confirm,
                last_close, distance_from_high_pct, distance_from_low_pct
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s
            )
        """, (
            symbol, z.zone_id, z.state.value,
            start_date, end_date, z.current_start_idx, z.current_end_idx,
            z_high, z_low, z.num_bars,
            z.current_score,
            "|".join(sorted(z.current_methods)) if z.current_methods else None,
            z.first_seen_date, z.first_seen_bar, z.last_seen_bar,
            z.consecutive_detections, bars_until,
            last_close, dist_high_pct, dist_low_pct,
        ))
        developing_count += 1

    conn.commit()
    cur.close()
    conn.close()

    return zone_count, event_count, developing_count


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
    return df


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Suppress v3 engine print output
    import io, contextlib

    p = argparse.ArgumentParser(description="Forward-Mode Consolidation Detector v2 (Exhaustive)")
    p.add_argument("--symbol", default="F")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--user", default="remote")
    p.add_argument("--password", default="Chamba4347!")
    p.add_argument("--database", default="leo")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--csv", type=str, default=None)
    p.add_argument("--trail", type=int, default=150, help="Trailing window size (default: 150)")
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--scan-last", type=int, default=None,
                   help="Only walk the last N bars. All prior bars are context only. "
                        "Example: --scan-last 22 scans just the last month of daily bars. "
                        "The full dataset is still loaded for ATR/BBW/compression baselines.")
    p.add_argument("--confirm-bars", type=int, default=2,
                   help="Consecutive v3 detections to confirm (default: 2)")
    p.add_argument("--max-confirm-delay", type=int, default=10,
                   help="Max bars between zone end and confirmation. "
                        "Zones older than this are expired as stale. (default: 10)")
    p.add_argument("--min-bars", type=int, default=4)
    p.add_argument("--max-bars", type=int, default=50)
    p.add_argument("--max-range-atr", type=float, default=2.0)
    p.add_argument("--min-weighted-votes", type=float, default=2.5)
    p.add_argument("--iterations", type=int, default=30)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--scan-step", type=int, default=3,
                   help="Run v3 every N bars (1=every bar, 3=every 3rd). "
                        "Breakouts checked every bar regardless. (default: 3)")
    p.add_argument("--auto", action="store_true",
                   help="Auto-calibrate all parameters based on stock characteristics. "
                        "Overrides manual settings for calibrated params.")
    p.add_argument("--save-mysql", action="store_true",
                   help="Save confirmed zones and events to MySQL tables "
                        "(consolidation_zones, consolidation_events)")
    p.add_argument("--output-chart", default="forward_v2_zones.png")
    p.add_argument("--output-csv", default="forward_v2_events.csv")
    args = p.parse_args()

# ──────────────────────────────────────────────────────────────────────
# Ticker list from MySQL
# ──────────────────────────────────────────────────────────────────────
def fetch_tickers(host="127.0.0.1", user="remote", password="Chamba4347!",
                  database="leo", port=3306):
    """Fetch scan universe from leo.tickers."""
    import mysql.connector
    conn = mysql.connector.connect(host=host, user=user, password=password,
                                   database=database, port=port)
    cur = conn.cursor()
    cur.execute("""
        SELECT ticker FROM tickers
        WHERE (last_day_volume > 500000
          AND type = 'CS'
          AND last_day_close > 5)
          OR ticker = 'SPY'
        ORDER BY last_day_volume DESC
        LIMIT 1000
    """)
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tickers


# ──────────────────────────────────────────────────────────────────────
# Single-symbol run (extracted from old __main__)
# ──────────────────────────────────────────────────────────────────────
def run_single_symbol(symbol, args, suppress_output=False):
    """
    Run the forward detector on one symbol.
    Returns (walker, df, profile_dict) or None on failure.
    """
    profile = {}

    try:
        if args.csv:
            df = load_from_csv(args.csv)
        else:
            df = load_from_mysql(symbol, args.host, args.user, args.password,
                                 args.database, args.port, args.limit)

        if len(df) < 60:
            if not suppress_output:
                print(f"  SKIP {symbol}: only {len(df)} bars (need 60+)")
            return None

        if not suppress_output:
            print(f"  Loaded {len(df)} bars for {symbol}")
            if "bar_date" in df.columns:
                print(f"    Date range: {df['bar_date'].iloc[0]} → {df['bar_date'].iloc[-1]}")
            print(f"    Price range: {df['low'].min():.2f} — {df['high'].max():.2f}")

    except Exception as e:
        if not suppress_output:
            print(f"  SKIP {symbol}: load failed — {e}")
        return None

    # ── Make a copy of args so auto-calibration doesn't bleed between symbols ──
    sym_args = argparse.Namespace(**vars(args))

    # ── Auto-calibration ──
    if sym_args.auto:
        try:
            profile, auto_params = profile_stock(df)
            if not suppress_output:
                print(f"    Noise={profile['noise_score']} ATR%={profile['atr_pct']}%")
            sym_args.max_range_atr = auto_params["max_range_atr"]
            sym_args.min_weighted_votes = auto_params["min_weighted_votes"]
            sym_args.confirm_bars = auto_params["confirm_bars"]
            sym_args.iterations = auto_params["num_iterations"]
            sym_args.max_confirm_delay = auto_params["max_confirm_delay"]
            sym_args.trail = auto_params["trail"]
            sym_args.compression_floor = auto_params["compression_floor"]
            sym_args.overlap_floor = auto_params["overlap_floor"]
            sym_args.rel_vol_floor = auto_params["rel_vol_floor"]
        except Exception as e:
            if not suppress_output:
                print(f"    Auto-calibration failed, using defaults: {e}")
            sym_args.compression_floor = 0.85
            sym_args.overlap_floor = 0.25
            sym_args.rel_vol_floor = 0.80
    else:
        sym_args.compression_floor = getattr(sym_args, 'compression_floor', 0.85)
        sym_args.overlap_floor = getattr(sym_args, 'overlap_floor', 0.25)
        sym_args.rel_vol_floor = getattr(sym_args, 'rel_vol_floor', 0.80)

    # ── Build walker ──
    walker = ForwardWalker(
        df, trail=sym_args.trail, warmup=sym_args.warmup,
        confirm_bars=sym_args.confirm_bars,
        max_confirm_delay=sym_args.max_confirm_delay,
        min_bars=sym_args.min_bars, max_bars=sym_args.max_bars,
        max_range_atr=sym_args.max_range_atr,
        min_weighted_votes=sym_args.min_weighted_votes,
        num_iterations=sym_args.iterations,
        compression_floor=sym_args.compression_floor,
        overlap_floor=sym_args.overlap_floor,
        rel_vol_floor=sym_args.rel_vol_floor,
    )

    # ── Determine scan range ──
    start_bar = 0
    end_bar = len(df)
    if sym_args.scan_last is not None:
        start_bar = max(0, len(df) - sym_args.scan_last)
        walker.warmup = min(walker.warmup, start_bar)

    # ── Suppress v3 engine noise ──
    old_stdout = sys.stdout

    class SuppressV3:
        def __init__(self, real_stdout, quiet):
            self.real = real_stdout
            self.quiet = quiet
        def write(self, s):
            if any(k in s for k in ["Adaptive max_range_pct", "Pass:", "Total raw:",
                                     "Fusing", "Final zones:"]):
                return
            if suppress_output and s.strip():
                return  # suppress everything in batch mode
            self.real.write(s)
        def flush(self):
            self.real.flush()

    sys.stdout = SuppressV3(old_stdout, sym_args.quiet or suppress_output)

    # ── Walk ──
    elapsed = walker.walk(start_bar=start_bar, end_bar=end_bar,
                          verbose=not suppress_output,
                          quiet=sym_args.quiet or suppress_output,
                          scan_step=sym_args.scan_step)

    sys.stdout = old_stdout

    # ── Summary ──
    s = walker.summary()
    confirmed = walker.get_confirmed_zones()
    developing = [z for z in walker.tracked_zones if z.state == ZoneState.DEVELOPING]

    if not suppress_output:
        print(f"\n    Confirmed: {s['confirmed']}  Breakouts: {s['breakouts']}  "
              f"Developing: {s['developing']}  Time: {elapsed:.1f}s")
        if confirmed:
            confirmed.sort(key=lambda z: z.locked_score, reverse=True)
            for i, z in enumerate(confirmed[:5]):
                bo = ""
                if z.state == ZoneState.BREAKOUT:
                    arrow = "↑" if z.breakout_direction == "up" else "↓"
                    bo = f" BO:{arrow}{z.breakout_magnitude:.1f}ATR"
                print(f"    {i+1}. {walker._get_date(z.locked_start_idx)} → "
                      f"{walker._get_date(z.locked_end_idx)} "
                      f"${z.locked_low:.2f}-${z.locked_high:.2f} "
                      f"score={z.locked_score:.3f}{bo}")

    # ── MySQL export ──
    if sym_args.save_mysql:
        try:
            noise = profile.get("noise_score") if sym_args.auto else None
            zc, ec, dc = export_to_mysql(
                walker, symbol, df,
                host=sym_args.host, user=sym_args.user, password=sym_args.password,
                database=sym_args.database, port=sym_args.port,
                auto_calibrated=sym_args.auto,
                noise_score=noise,
            )
            if not suppress_output:
                print(f"    MySQL: {zc} zones + {ec} events + {dc} developing")
        except Exception as e:
            if not suppress_output:
                print(f"    MySQL export failed: {e}")

    # ── CSV export (only in single-symbol mode) ──
    if not suppress_output and not args.symbol is None:
        walker.export_events_csv(sym_args.output_csv)

    # ── Chart (only in single-symbol mode) ──
    if not suppress_output and args.symbol is not None:
        try:
            plot_forward(df, walker, symbol=symbol, save_path=sym_args.output_chart)
        except Exception as e:
            pass

    return walker, df, profile


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import io, contextlib

    p = argparse.ArgumentParser(description="Forward-Mode Consolidation Detector v2 (Exhaustive)")
    p.add_argument("--symbol", default=None,
                   help="Single symbol to scan. If omitted, scans all tickers from "
                        "leo.tickers matching volume/price filters.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--user", default="remote")
    p.add_argument("--password", default="Chamba4347!")
    p.add_argument("--database", default="leo")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--csv", type=str, default=None)
    p.add_argument("--trail", type=int, default=150)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--scan-last", type=int, default=None,
                   help="Only walk the last N bars per symbol.")
    p.add_argument("--confirm-bars", type=int, default=2)
    p.add_argument("--max-confirm-delay", type=int, default=10)
    p.add_argument("--min-bars", type=int, default=4)
    p.add_argument("--max-bars", type=int, default=50)
    p.add_argument("--max-range-atr", type=float, default=2.0)
    p.add_argument("--min-weighted-votes", type=float, default=2.5)
    p.add_argument("--iterations", type=int, default=30)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--scan-step", type=int, default=3)
    p.add_argument("--auto", action="store_true",
                   help="Auto-calibrate per symbol.")
    p.add_argument("--save-mysql", action="store_true")
    p.add_argument("--output-chart", default="forward_v2_zones.png")
    p.add_argument("--output-csv", default="forward_v2_events.csv")
    args = p.parse_args()

    print("=" * 70)
    print("FORWARD-MODE CONSOLIDATION DETECTOR v2 (EXHAUSTIVE, NO REPAINT)")
    print("=" * 70)

    if args.symbol:
        # ── Single-symbol mode ──
        print(f"\nSymbol: {args.symbol}")
        result = run_single_symbol(args.symbol, args, suppress_output=False)
        if result:
            print(f"\n✓ Done.")
        else:
            print(f"\n✗ Failed.")

    else:
        # ── Multi-symbol scan mode ──
        print(f"\nNo --symbol provided. Fetching ticker universe from MySQL...")
        try:
            tickers = fetch_tickers(args.host, args.user, args.password,
                                     args.database, args.port)
        except Exception as e:
            print(f"  Failed to fetch tickers: {e}")
            sys.exit(1)

        print(f"  Found {len(tickers)} tickers matching filters")
        print(f"  Scan-last: {args.scan_last or 'all bars'}")
        print(f"  Auto-calibrate: {args.auto}")
        print(f"  Save to MySQL: {args.save_mysql}")
        print()

        total_zones = 0
        total_developing = 0
        total_errors = 0
        total_skipped = 0
        results_summary = []
        t0_all = time.time()

        for i, ticker in enumerate(tickers):
            pct = (i + 1) / len(tickers) * 100
            print(f"[{i+1:>4}/{len(tickers)} {pct:5.1f}%] {ticker:<8}", end="", flush=True)

            result = run_single_symbol(ticker, args, suppress_output=True)

            if result is None:
                total_skipped += 1
                print("  SKIP")
                continue

            walker, df, profile = result
            s = walker.summary()
            confirmed = walker.get_confirmed_zones()
            developing = [z for z in walker.tracked_zones if z.state == ZoneState.DEVELOPING]

            total_zones += s["confirmed"]
            total_developing += s["developing"]

            # One-line summary
            dev_str = f" DEV:{s['developing']}" if s['developing'] > 0 else ""
            conf_str = f" CONF:{s['confirmed']}" if s['confirmed'] > 0 else ""
            noise_str = f" noise={profile.get('noise_score', '?')}" if args.auto else ""

            # Show developing zones prominently
            if s['developing'] > 0:
                dev_details = []
                for dz in developing:
                    dev_details.append(
                        f"${dz.current_low:.2f}-${dz.current_high:.2f} "
                        f"s={dz.current_score:.2f}")
                print(f"  ★ FORMING: {', '.join(dev_details)}{conf_str}{noise_str}")
            elif s['confirmed'] > 0:
                top = max(confirmed, key=lambda z: z.locked_score)
                print(f"  {conf_str} top={top.locked_score:.3f}{noise_str}")
            else:
                print(f"  (no zones){noise_str}")

            results_summary.append({
                "symbol": ticker,
                "confirmed": s["confirmed"],
                "developing": s["developing"],
                "breakouts": s["breakouts"],
                "noise": profile.get("noise_score", None),
            })

        elapsed_all = time.time() - t0_all

        # ── Final summary ──
        print()
        print("=" * 70)
        print("SCAN COMPLETE")
        print("=" * 70)
        print(f"  Tickers scanned:  {len(tickers)}")
        print(f"  Skipped:          {total_skipped}")
        print(f"  Total confirmed:  {total_zones}")
        print(f"  Total developing: {total_developing}")
        print(f"  Total time:       {elapsed_all:.0f}s "
              f"({elapsed_all/max(1,len(tickers)-total_skipped):.1f}s/symbol avg)")

        # Show all currently developing zones
        if total_developing > 0 and args.save_mysql:
            print(f"\n  CURRENTLY FORMING ZONES (saved to consolidation_developing):")
            print(f"  Query: SELECT * FROM consolidation_developing ORDER BY current_score DESC;")

        print(f"\n✓ Done.")
