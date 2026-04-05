#!/usr/bin/env python3
"""
Find consolidation zones for a single symbol and save them to MySQL.

Usage
-----
    python consolidation_zones.py --symbol AAPL
    python consolidation_zones.py --symbol TT --target-pct 10
    python consolidation_zones.py --symbol TT --target-pct 5 --window 10

How it works
------------
1. Compute a rolling ATR (Average True Range) as a volatility baseline for
   each bar.
2. For each sliding window of N bars, measure how tight the window's actual
   price range is RELATIVE to what ATR predicts.  This "compression ratio"
   adapts automatically to each stock's own volatility regime.
3. Flag the tightest N% of windows (default 10%).
4. **Merge overlapping/adjacent qualifying windows** into distinct zones —
   this is critical because a 5-bar slide produces tons of 1-bar-shifted
   duplicates that are really the same consolidation period.
5. Save only the merged zones to MySQL.

This produces a clean, usable set of distinct consolidation periods.
"""

import argparse
import statistics
from typing import List, Tuple, Dict, Any, Optional

import mysql.connector

from polygon_download import get_db_connection, TABLE_NAME


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

def fetch_bars(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch ASC-ordered bar data for the symbol.
    Returns list of dicts with keys: date, open, high, low, close, volume.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT bar_date, open, high, low, close, volume
                FROM `{TABLE_NAME}`
                WHERE symbol = %s
                ORDER BY bar_date ASC
                """,
                (symbol,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "date": row[0],
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# ATR computation
# ---------------------------------------------------------------------------

def compute_atr(bars: List[Dict[str, Any]], period: int = 20) -> List[Optional[float]]:
    """
    Compute rolling ATR.  Returns a list the same length as bars.
    Entries before `period` are None (not enough data).
    """
    atrs: List[Optional[float]] = [None] * len(bars)

    for i in range(period, len(bars)):
        trs = []
        for j in range(i - period + 1, i + 1):
            hi = bars[j]["high"]
            lo = bars[j]["low"]
            prev_c = bars[j - 1]["close"]
            tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c))
            trs.append(tr)
        atrs[i] = sum(trs) / len(trs)

    return atrs


# ---------------------------------------------------------------------------
# Window scanning — ATR-relative compression ratio
# ---------------------------------------------------------------------------

def scan_windows(
    bars: List[Dict[str, Any]],
    atrs: List[Optional[float]],
    window: int,
) -> List[Dict[str, Any]]:
    """
    For each window of `window` bars, compute how compressed the actual
    price range is relative to what ATR predicts.

    compression_ratio = actual_range / (ATR * sqrt(window))
      - Values < 1.0 mean tighter than expected (consolidation)
      - Values > 1.0 mean wider than expected (trending/volatile)

    Also computes close_cv and volume trend as secondary signals.
    """
    import math

    results = []
    for end_idx in range(window - 1, len(bars)):
        start_idx = end_idx - window + 1

        # Need ATR at the start of the window
        atr_val = atrs[start_idx]
        if atr_val is None or atr_val == 0:
            continue

        w = bars[start_idx : end_idx + 1]

        # Actual range
        highest = max(b["high"] for b in w)
        lowest = min(b["low"] for b in w)
        actual_range = highest - lowest
        mid = (highest + lowest) / 2.0

        # Expected range: ATR * sqrt(window)
        expected_range = atr_val * math.sqrt(window)

        compression_ratio = actual_range / expected_range

        # Secondary: close CV
        closes = [b["close"] for b in w]
        mean_close = sum(closes) / len(closes)
        std_close = statistics.stdev(closes) if len(closes) > 1 else 0.0
        close_cv = (std_close / mean_close) if mean_close != 0 else 0.0

        # Secondary: volume declining?
        half = len(w) // 2
        if half > 0:
            vol_first = sum(b["volume"] for b in w[:half]) / half
            vol_second = sum(b["volume"] for b in w[half:]) / len(w[half:])
            volume_declining = vol_second < vol_first
        else:
            volume_declining = False

        # Secondary: wick compression
        wick_compressed = (w[0]["high"] - w[0]["low"]) > (w[-1]["high"] - w[-1]["low"])

        range_pct = actual_range / mid if mid != 0 else 0.0

        results.append({
            "start_date": w[0]["date"],
            "end_date": w[-1]["date"],
            "start_idx": start_idx,
            "end_idx": end_idx,
            "compression_ratio": compression_ratio,
            "range_pct": round(range_pct, 6),
            "close_cv": round(close_cv, 6),
            "actual_range": round(actual_range, 2),
            "expected_range": round(expected_range, 2),
            "mid_price": round(mid, 2),
            "wick_compressed": wick_compressed,
            "volume_declining": volume_declining,
        })

    return results


# ---------------------------------------------------------------------------
# Zone merging — collapse overlapping windows into distinct zones
# ---------------------------------------------------------------------------

def merge_into_zones(
    qualifying_windows: List[Dict[str, Any]],
    bars: List[Dict[str, Any]],
    gap_bars: int = 1,
) -> List[Dict[str, Any]]:
    """
    Merge overlapping or nearly-adjacent qualifying windows into distinct
    consolidation zones.

    gap_bars: max gap between windows to still merge them (default 1 =
              windows touching or overlapping get merged).

    Returns a list of zone dicts with start/end dates, duration, and
    best (lowest) compression ratio within the zone.
    """
    if not qualifying_windows:
        return []

    # Sort by start index
    sorted_wins = sorted(qualifying_windows, key=lambda w: w["start_idx"])

    zones = []
    current = {
        "start_idx": sorted_wins[0]["start_idx"],
        "end_idx": sorted_wins[0]["end_idx"],
        "best_ratio": sorted_wins[0]["compression_ratio"],
        "all_ratios": [sorted_wins[0]["compression_ratio"]],
        "all_range_pcts": [sorted_wins[0]["range_pct"]],
        "all_cvs": [sorted_wins[0]["close_cv"]],
    }

    for w in sorted_wins[1:]:
        if w["start_idx"] <= current["end_idx"] + gap_bars:
            # Overlaps or adjacent — extend
            current["end_idx"] = max(current["end_idx"], w["end_idx"])
            current["best_ratio"] = min(current["best_ratio"], w["compression_ratio"])
            current["all_ratios"].append(w["compression_ratio"])
            current["all_range_pcts"].append(w["range_pct"])
            current["all_cvs"].append(w["close_cv"])
        else:
            # Gap — save current zone, start new one
            zones.append(current)
            current = {
                "start_idx": w["start_idx"],
                "end_idx": w["end_idx"],
                "best_ratio": w["compression_ratio"],
                "all_ratios": [w["compression_ratio"]],
                "all_range_pcts": [w["range_pct"]],
                "all_cvs": [w["close_cv"]],
            }
    zones.append(current)

    # Enrich with dates, prices, duration
    enriched = []
    for z in zones:
        zone_bars = bars[z["start_idx"] : z["end_idx"] + 1]
        highest = max(b["high"] for b in zone_bars)
        lowest = min(b["low"] for b in zone_bars)
        mid = (highest + lowest) / 2.0

        enriched.append({
            "start_date": bars[z["start_idx"]]["date"],
            "end_date": bars[z["end_idx"]]["date"],
            "duration_bars": z["end_idx"] - z["start_idx"] + 1,
            "best_compression": round(z["best_ratio"], 4),
            "avg_compression": round(sum(z["all_ratios"]) / len(z["all_ratios"]), 4),
            "range_pct": round((highest - lowest) / mid, 6) if mid else 0.0,
            "close_cv": round(sum(z["all_cvs"]) / len(z["all_cvs"]), 6),
            "high": round(highest, 2),
            "low": round(lowest, 2),
            "mid": round(mid, 2),
        })

    return enriched


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

CONSOLIDATION_TABLE = "consolidation_zones"

CREATE_CONSOLIDATION_SQL = f"""
CREATE TABLE IF NOT EXISTS `{CONSOLIDATION_TABLE}` (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol            VARCHAR(20)   NOT NULL,
    start_date        DATE          NOT NULL,
    end_date          DATE          NOT NULL,
    window_size       INT           NOT NULL,
    range_pct         DECIMAL(18,6) NULL,
    close_cv          DECIMAL(18,6) NULL,
    signals_met       INT           NOT NULL DEFAULT 0,
    wick_compressed   TINYINT(1)    NOT NULL DEFAULT 0,
    volume_declining  TINYINT(1)    NOT NULL DEFAULT 0,
    is_consolidation  TINYINT(1)    NOT NULL DEFAULT 0,
    created_at        DATETIME      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sym_dates (symbol, start_date, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_consolidation_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_CONSOLIDATION_SQL)
        try:
            cur.execute(
                f"ALTER TABLE `{CONSOLIDATION_TABLE}` "
                "ADD COLUMN signals_met INT NOT NULL DEFAULT 0 "
                "AFTER close_cv"
            )
        except mysql.connector.errors.ProgrammingError:
            pass
    conn.commit()


def save_zones(symbol: str, zones: List[Dict[str, Any]], window: int) -> None:
    """Delete existing rows for symbol and insert merged zones."""
    conn = get_db_connection()
    try:
        ensure_consolidation_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM `{CONSOLIDATION_TABLE}` WHERE symbol = %s",
                (symbol,),
            )
            if zones:
                insert_sql = f"""
                INSERT INTO `{CONSOLIDATION_TABLE}`
                    (symbol, start_date, end_date, window_size,
                     range_pct, close_cv, signals_met,
                     wick_compressed, volume_declining, is_consolidation)
                VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                rows = [
                    (
                        symbol,
                        z["start_date"],
                        z["end_date"],
                        z["duration_bars"],
                        z["range_pct"],
                        z["close_cv"],
                        0,   # signals_met (legacy)
                        0,   # wick_compressed (legacy)
                        0,   # volume_declining (legacy)
                        1,   # is_consolidation
                    )
                    for z in zones
                ]
                cur.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def scan_consolidations(
    symbol: str,
    window: int = 5,
    atr_period: int = 20,
    target_pct: float = 10.0,
    gap_bars: int = 1,
) -> None:
    bars = fetch_bars(symbol)
    if not bars:
        print(f"No bars found in `{TABLE_NAME}` for symbol {symbol}.")
        return

    if window < 2:
        print("Window size must be at least 2.")
        return

    print(f"\n{'='*70}")
    print(f"  CONSOLIDATION ZONE SCANNER for {symbol}")
    print(f"  {len(bars)} bars  |  window={window}  |  ATR period={atr_period}")
    print(f"  Target: tightest {target_pct:.0f}% of windows → merge into zones")
    print(f"{'='*70}")

    # Step 1: Compute ATR
    print("  Computing ATR...")
    atrs = compute_atr(bars, period=atr_period)

    # Step 2: Scan all windows
    print("  Scanning windows...")
    all_windows = scan_windows(bars, atrs, window)
    print(f"  → {len(all_windows)} windows analysed")

    if not all_windows:
        print("  No valid windows (need more data for ATR warmup).")
        return

    # Step 3: Find cutoff and flag qualifying windows
    ratios = [w["compression_ratio"] for w in all_windows]
    ratios_sorted = sorted(ratios)
    cutoff_idx = max(0, int(len(ratios_sorted) * target_pct / 100.0) - 1)
    cutoff = ratios_sorted[cutoff_idx]

    qualifying = [w for w in all_windows if w["compression_ratio"] <= cutoff]
    print(f"  → {len(qualifying)} windows qualify "
          f"(compression ratio ≤ {cutoff:.2f})")

    # Step 4: Merge into distinct zones
    zones = merge_into_zones(qualifying, bars, gap_bars=gap_bars)
    print(f"  → Merged into {len(zones)} distinct consolidation zones")

    # Distribution info
    print(f"\n  Compression ratio distribution (all windows):")
    for p in [5, 10, 25, 50, 75, 90]:
        idx = int(len(ratios_sorted) * p / 100)
        print(f"    P{p:2d}: {ratios_sorted[idx]:.2f}")

    # Print zones
    print(f"\n{'='*70}")
    print(f"  CONSOLIDATION ZONES ({len(zones)} found)")
    print(f"{'='*70}")
    print(
        f"  {'Start':<12} {'End':<12} {'Days':>5} "
        f"{'Range%':>8} {'CV':>8} {'Compress':>9} "
        f"{'Low':>9} {'High':>9}"
    )
    print(f"  {'-'*12} {'-'*12} {'-'*5} {'-'*8} {'-'*8} {'-'*9} {'-'*9} {'-'*9}")

    for z in zones:
        print(
            f"  {z['start_date']!s:<12} {z['end_date']!s:<12} "
            f"{z['duration_bars']:>5} "
            f"{z['range_pct']*100:>7.2f}% "
            f"{z['close_cv']*100:>7.3f}% "
            f"{z['best_compression']:>9.2f} "
            f"${z['low']:>8.2f} "
            f"${z['high']:>8.2f}"
        )

    print(f"\n{'='*70}")

    # Save to MySQL
    save_zones(symbol, zones, window)
    print(f"  ✓ Saved {len(zones)} zones to MySQL ({CONSOLIDATION_TABLE})")
    print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Scan for consolidation zones from daily_ohlcv."
    )
    parser.add_argument(
        "--symbol", required=True,
        help="Ticker, e.g. --symbol AAPL",
    )
    parser.add_argument(
        "--window", type=int, default=5,
        help="Sliding window size in bars (default: 5).",
    )
    parser.add_argument(
        "--atr-period", type=int, default=20,
        help="ATR lookback period (default: 20).",
    )
    parser.add_argument(
        "--target-pct", type=float, default=10.0,
        help="Flag the tightest N%% of windows before merging "
             "(default: 10). Lower = stricter.",
    )
    parser.add_argument(
        "--gap-bars", type=int, default=1,
        help="Max gap in bars to still merge adjacent zones (default: 1).",
    )
    args = parser.parse_args()

    scan_consolidations(
        args.symbol.upper(),
        window=args.window,
        atr_period=args.atr_period,
        target_pct=args.target_pct,
        gap_bars=args.gap_bars,
    )


if __name__ == "__main__":
    main()