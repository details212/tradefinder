"""
Update high-volume tickers in the MySQL `tickers` table with
fundamental data from Yahoo Finance (via yfinance).

Selection:
  - last_day_volume > MIN_VOLUME (default: 1,000,000)

Data source:
  - yfinance Ticker.get_info()

Fields populated (columns in `tickers`):
  - trailing_pe
  - forward_pe
  - price_to_book
  - price_to_sales_ttm
  - enterprise_to_ebitda
  - enterprise_to_revenue
  - earnings_growth
  - revenue_growth
  - gross_margins
  - operating_margins
  - profit_margins
  - trailing_eps
  - forward_eps
  - earnings_quarterly_growth
  - debt_to_equity
  - current_ratio
  - quick_ratio
  - total_cash
  - total_debt
  - short_ratio
  - short_percent_of_float
  - market_cap
  - average_volume
  - average_daily_volume_10_day
  - held_percent_institutions
  - held_percent_insiders
  - float_shares
  - target_mean_price
  - recommendation_mean
  - number_of_analyst_opinions
  - fundamentals_updated_at
"""

import argparse
import math
import time
from typing import Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error

import yfinance as yf


# MySQL Database Configuration
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "remote",
    "password": "Chamba4347!",
    "database": "leo",
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
}


class YahooFundamentalsUpdater:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.conn: Optional[mysql.connector.MySQLConnection] = None
        self.cursor: Optional[mysql.connector.cursor.MySQLCursor] = None

        # Mapping: db_column -> (yfinance_info_key, field_type)
        # field_type: "float", "bigint", "int"
        self.field_map: Dict[str, Tuple[str, str]] = {
            # Valuation
            "trailing_pe": ("trailingPE", "float"),
            "forward_pe": ("forwardPE", "float"),
            "price_to_book": ("priceToBook", "float"),
            "price_to_sales_ttm": ("priceToSalesTrailing12Months", "float"),
            "enterprise_to_ebitda": ("enterpriseToEbitda", "float"),
            "enterprise_to_revenue": ("enterpriseToRevenue", "float"),
            # Growth & profitability
            "earnings_growth": ("earningsGrowth", "float"),
            "revenue_growth": ("revenueGrowth", "float"),
            "gross_margins": ("grossMargins", "float"),
            "operating_margins": ("operatingMargins", "float"),
            "profit_margins": ("profitMargins", "float"),
            # Earnings & EPS
            "trailing_eps": ("trailingEps", "float"),
            "forward_eps": ("forwardEps", "float"),
            "earnings_quarterly_growth": ("earningsQuarterlyGrowth", "float"),
            # Financial health
            "debt_to_equity": ("debtToEquity", "float"),
            "current_ratio": ("currentRatio", "float"),
            "quick_ratio": ("quickRatio", "float"),
            "total_cash": ("totalCash", "bigint"),
            "total_debt": ("totalDebt", "bigint"),
            # Short interest / sentiment
            "short_ratio": ("shortRatio", "float"),
            "short_percent_of_float": ("shortPercentOfFloat", "float"),
            # Size & liquidity
            "market_cap": ("marketCap", "bigint"),
            "average_volume": ("averageVolume", "bigint"),
            "average_daily_volume_10_day": ("averageDailyVolume10Day", "bigint"),
            # Ownership
            "held_percent_institutions": ("heldPercentInstitutions", "float"),
            "held_percent_insiders": ("heldPercentInsiders", "float"),
            "float_shares": ("floatShares", "bigint"),
            # Analyst data
            "target_mean_price": ("targetMeanPrice", "float"),
            "recommendation_mean": ("recommendationMean", "float"),
            "number_of_analyst_opinions": ("numberOfAnalystOpinions", "int"),
        }

    def connect_db(self) -> bool:
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to MySQL database")
            return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")

    def ensure_columns(self):
        """Ensure all fundamental columns exist on the `tickers` table."""
        column_defs = {
            # Floats / ratios
            "trailing_pe": "DOUBLE",
            "forward_pe": "DOUBLE",
            "price_to_book": "DOUBLE",
            "price_to_sales_ttm": "DOUBLE",
            "enterprise_to_ebitda": "DOUBLE",
            "enterprise_to_revenue": "DOUBLE",
            "earnings_growth": "DOUBLE",
            "revenue_growth": "DOUBLE",
            "gross_margins": "DOUBLE",
            "operating_margins": "DOUBLE",
            "profit_margins": "DOUBLE",
            "trailing_eps": "DOUBLE",
            "forward_eps": "DOUBLE",
            "earnings_quarterly_growth": "DOUBLE",
            "debt_to_equity": "DOUBLE",
            "current_ratio": "DOUBLE",
            "quick_ratio": "DOUBLE",
            "short_ratio": "DOUBLE",
            "short_percent_of_float": "DOUBLE",
            "held_percent_institutions": "DOUBLE",
            "held_percent_insiders": "DOUBLE",
            "target_mean_price": "DOUBLE",
            "recommendation_mean": "DOUBLE",
            # Integers / bigints
            "total_cash": "BIGINT",
            "total_debt": "BIGINT",
            "market_cap": "BIGINT",
            "average_volume": "BIGINT",
            "average_daily_volume_10_day": "BIGINT",
            "float_shares": "BIGINT",
            "number_of_analyst_opinions": "INT",
            # Metadata
            "fundamentals_updated_at": "DATETIME",
        }

        self.cursor.execute("SHOW COLUMNS FROM tickers")
        existing = {row[0] for row in self.cursor.fetchall()}

        for col, col_type in column_defs.items():
            if col not in existing:
                try:
                    sql = f"ALTER TABLE tickers ADD COLUMN {col} {col_type} NULL"
                    self.cursor.execute(sql)
                    self.conn.commit()
                    print(f"  ✓ Added column: {col}")
                except Error as e:
                    print(f"  ⚠ Could not add column {col}: {e}")

    def get_high_volume_tickers(self, min_volume: int, min_price: float, type_filter: str) -> List[str]:
        sql = """
        SELECT ticker
        FROM tickers
        WHERE last_day_volume > %s
          AND last_day_close > %s
          AND type = %s
        """
        self.cursor.execute(sql, (min_volume, min_price, type_filter))
        rows = self.cursor.fetchall()
        tickers = [r[0] for r in rows]
        print(
            f"✓ Found {len(tickers)} tickers with "
            f"last_day_volume > {min_volume:,}, "
            f"last_day_close > {min_price}, "
            f"type = '{type_filter}'"
        )
        return tickers

    @staticmethod
    def polygon_to_yahoo_symbol(ticker: str) -> str:
        """
        Convert Polygon-style symbols to Yahoo format when needed.
        Example: 'BRK.B' -> 'BRK-B'
        """
        return ticker.replace(".", "-")

    @staticmethod
    def normalize_value(value, field_type: str):
        if value is None:
            return None

        # Handle NaN / +/-inf (from numpy / pandas / Python floats)
        if isinstance(value, (float, int)):
            f = float(value)
            if math.isnan(f) or not math.isfinite(f):
                return None

        # Sometimes yfinance returns strings like "NaN", "inf", etc.
        if isinstance(value, str):
            lv = value.strip().lower()
            if lv in ("nan", "inf", "infinity", "-inf", "-infinity"):
                return None

        try:
            if field_type == "float":
                return float(value)
            if field_type in ("bigint", "int"):
                return int(value)
        except Exception:
            return None

        return None

    def update_fundamentals_for_ticker(self, ticker: str, info: Dict) -> None:
        cols = []
        vals = []

        for db_col, (info_key, field_type) in self.field_map.items():
            raw_val = info.get(info_key)
            norm_val = self.normalize_value(raw_val, field_type)
            cols.append(db_col)
            vals.append(norm_val)

        # fundamentals_updated_at timestamp
        cols.append("fundamentals_updated_at")
        vals.append(time.strftime("%Y-%m-%d %H:%M:%S"))

        set_clause = ", ".join(f"{c} = %s" for c in cols)
        sql = f"UPDATE tickers SET {set_clause} WHERE ticker = %s"

        vals.append(ticker)

        self.cursor.execute(sql, tuple(vals))

    def run(
        self,
        min_volume: int,
        min_price: float,
        type_filter: str,
        sleep_seconds: float = 0.5,
        max_tickers: Optional[int] = None,
    ):
        self.ensure_columns()

        tickers = self.get_high_volume_tickers(min_volume, min_price, type_filter)
        if max_tickers is not None:
            tickers = tickers[:max_tickers]

        total = len(tickers)
        if total == 0:
            print("No tickers to update.")
            return

        print(f"\nUpdating fundamentals for {total} tickers via Yahoo Finance...")

        updated = 0
        errors = 0
        start_time = time.time()

        for i, ticker in enumerate(tickers, 1):
            yahoo_symbol = self.polygon_to_yahoo_symbol(ticker)
            try:
                yt = yf.Ticker(yahoo_symbol)
                info = yt.get_info()

                if not info:
                    print(f"  ⚠ No info for {ticker} ({yahoo_symbol})")
                else:
                    self.update_fundamentals_for_ticker(ticker, info)
                    updated += 1

            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ✗ Error updating {ticker} ({yahoo_symbol}): {e}")

            if i % 50 == 0:
                self.conn.commit()
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0.0
                print(
                    f"  Progress: {i}/{total} "
                    f"(updated {updated}, errors {errors}) "
                    f"| Rate: {rate:.2f}/sec"
                )

            # Be gentle with Yahoo's servers
            time.sleep(sleep_seconds)

        self.conn.commit()
        elapsed = time.time() - start_time
        rate = total / elapsed if elapsed > 0 else 0.0

        print("\n✓ Fundamentals update complete!")
        print(f"  Tickers processed: {total}")
        print(f"  Updated: {updated}")
        print(f"  Errors: {errors}")
        print(f"  Time: {elapsed:.1f} seconds (rate: {rate:.2f}/sec)")


def main():
    parser = argparse.ArgumentParser(
        description="Update high-volume tickers in MySQL with Yahoo Finance fundamentals"
    )
    parser.add_argument(
        "--min-volume",
        type=int,
        default=1_000_000,
        help="Minimum last_day_volume to select tickers (default: 1,000,000)",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=5.0,
        help="Minimum last_day_close price to select tickers (default: 5.0)",
    )
    parser.add_argument(
        "--type",
        dest="type_filter",
        type=str,
        default="CS",
        help="Ticker type filter, e.g. 'CS' for common stock (default: CS)",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Optional cap on number of tickers to process (default: no limit)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Sleep between Yahoo requests in seconds (default: 0.5)",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("  YAHOO FUNDAMENTALS UPDATER")
    print("=" * 80)

    updater = YahooFundamentalsUpdater(DB_CONFIG)

    try:
        if not updater.connect_db():
            return

        updater.run(
            min_volume=args.min_volume,
            min_price=args.min_price,
            type_filter=args.type_filter,
            sleep_seconds=args.sleep,
            max_tickers=args.max_tickers,
        )
    finally:
        updater.close()


if __name__ == "__main__":
    main()

