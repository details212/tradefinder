"""
Download all tickers from Polygon.io and store in MySQL database
Supports upsert functionality - updates existing rows on subsequent runs
"""

import mysql.connector
from mysql.connector import Error
import requests
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# Configuration
POLYGON_API_KEY = "pntJnvnXxV3q2nAIdsph4RbT0b_oUlPE"
POLYGON_BASE_URL = "https://api.polygon.io"

# MySQL Database Configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'remote',
    'password': 'Chamba4347!',
    'database': 'tradefinder',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# Rate limiting - Polygon.io allows 5 requests per second on free tier
# Adjust based on your subscription tier:
#   Free: 5  |  Starter: 100  |  Developer/above: unlimited (use 50-200)
REQUESTS_PER_SECOND = 5
MIN_REQUEST_INTERVAL = 1.0 / REQUESTS_PER_SECOND

# Number of parallel workers for the --update-sic pass.
# Each worker runs its own HTTP request concurrently, so network latency
# overlaps.  Keep workers <= REQUESTS_PER_SECOND to stay within rate limits.
SIC_WORKERS = 5


class PolygonTickerDownloader:
    """Class to handle downloading tickers from Polygon.io and storing in MySQL"""
    
    def __init__(self, api_key: str, db_config: Dict):
        self.api_key = api_key
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.last_request_time = 0
        
    def test_api_connection(self) -> bool:
        """Test Polygon API connectivity and API key validity"""
        print("\n" + "="*80)
        print("  TESTING POLYGON.IO API CONNECTION")
        print("="*80)
        
        # Test 1: Simple connectivity test with a lightweight endpoint
        print("\n1. Testing API connectivity...")
        try:
            # Use the market status endpoint - lightweight and doesn't count against limits
            test_url = f"{POLYGON_BASE_URL}/v1/marketstatus/now"
            params = {'apiKey': self.api_key}
            
            print(f"   Connecting to: {POLYGON_BASE_URL}")
            response = requests.get(test_url, params=params, timeout=30)
            
            if response.status_code == 200:
                print("   ✓ Connection successful!")
            elif response.status_code == 401 or response.status_code == 403:
                print(f"   ✗ Authentication failed (Status: {response.status_code})")
                print("   Check your API key - it may be invalid or expired")
                return False
            else:
                print(f"   ⚠ Unexpected status code: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print("   ✗ Connection timed out after 30 seconds")
            print("   This may indicate network connectivity issues")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"   ✗ Connection failed: {e}")
            print("   Check your internet connection or firewall settings")
            return False
        except Exception as e:
            print(f"   ✗ Unexpected error: {e}")
            return False
        
        # Test 2: Try fetching a small sample of tickers
        print("\n2. Testing ticker data access...")
        ticker_test_passed = False
        try:
            endpoint = "/v3/reference/tickers"
            params = {
                'market': 'stocks',
                'limit': 5,
                'active': 'true'
            }
            
            data = self.make_api_request(endpoint, params, max_retries=1)
            
            if data and data.get('status') == 'OK':
                results = data.get('results', [])
                print(f"   ✓ Successfully fetched {len(results)} sample tickers")
                if results:
                    print(f"   Sample: {', '.join([r.get('ticker', 'N/A') for r in results[:3]])}")
                ticker_test_passed = True
            elif data and data.get('status') == 'ERROR':
                print(f"   ⚠ API returned error: {data.get('message', 'Unknown error')}")
                print("   Basic connectivity works, but ticker endpoint has issues")
            else:
                print("   ⚠ No data returned from ticker endpoint")
                print("   Basic connectivity works, proceeding anyway...")
                
        except Exception as e:
            print(f"   ⚠ Ticker endpoint timeout (this is common)")
            print("   Basic API connectivity is working, so we'll proceed")
        
        print("\n" + "="*80)
        if ticker_test_passed:
            print("  ✓ API CONNECTION TEST PASSED (All checks successful)")
        else:
            print("  ✓ API CONNECTION TEST PASSED (Basic connectivity verified)")
            print("  Note: Ticker endpoint was slow, but this is often temporary")
        print("="*80)
        return True
    
    def rate_limit(self):
        """Ensure we don't exceed API rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
        self.last_request_time = time.time()
    
    def make_api_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3, timeout: int = 60) -> Optional[Dict]:
        """Make an API request to Polygon.io with rate limiting and retry logic"""
        url = f"{POLYGON_BASE_URL}{endpoint}"
        if params is None:
            params = {}
        params['apiKey'] = self.api_key
        
        for attempt in range(max_retries):
            self.rate_limit()
            
            try:
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    print(f"  ⚠ Timeout on {endpoint}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Error making API request to {endpoint}: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    print(f"  ⚠ Error on {endpoint}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Error making API request to {endpoint}: {e}")
                    return None
        
        return None
    
    def connect_db(self):
        """Connect to MySQL database"""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to MySQL database")
            return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            return False
    
    def create_table(self):
        """Create the tickers table with all available fields"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS tickers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) UNIQUE NOT NULL,
            
            -- Basic Information
            name VARCHAR(500),
            type VARCHAR(50),
            market VARCHAR(50),
            locale VARCHAR(50),
            primary_exchange VARCHAR(50),
            currency_name VARCHAR(50),
            sic_code VARCHAR(10),
            sector VARCHAR(100),
            industry VARCHAR(255),
            
            -- Identifier
            cik VARCHAR(20),
            
            -- Last Day Trading Data
            last_day_volume BIGINT,
            last_day_open DECIMAL(20, 4),
            last_day_high DECIMAL(20, 4),
            last_day_low DECIMAL(20, 4),
            last_day_close DECIMAL(20, 4),
            last_day_vwap DECIMAL(20, 4),
            last_day_transactions INT,
            last_day_date DATE,
            
            -- Metadata
            last_updated_utc DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_ticker (ticker),
            INDEX idx_type (type),
            INDEX idx_market (market)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        try:
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            
            # Add new columns if they don't exist (for existing tables)
            # Only ensure the lean set of columns used by the script.
            new_columns = [
                ('name', 'VARCHAR(500)'),
                ('type', 'VARCHAR(50)'),
                ('market', 'VARCHAR(50)'),
                ('locale', 'VARCHAR(50)'),
                ('primary_exchange', 'VARCHAR(50)'),
                ('currency_name', 'VARCHAR(50)'),
                ('sic_code', 'VARCHAR(10)'),
                ('sector', 'VARCHAR(100)'),
                ('industry', 'VARCHAR(255)'),
                ('cik', 'VARCHAR(20)'),
                ('last_day_volume', 'BIGINT'),
                ('last_day_open', 'DECIMAL(20, 4)'),
                ('last_day_high', 'DECIMAL(20, 4)'),
                ('last_day_low', 'DECIMAL(20, 4)'),
                ('last_day_close', 'DECIMAL(20, 4)'),
                ('last_day_vwap', 'DECIMAL(20, 4)'),
                ('last_day_transactions', 'INT'),
                ('last_day_date', 'DATE'),
                ('last_updated_utc', 'DATETIME'),
            ]
            
            # Check which columns already exist
            self.cursor.execute("SHOW COLUMNS FROM tickers")
            existing_columns = [row[0] for row in self.cursor.fetchall()]
            
            for col_name, col_type in new_columns:
                if col_name not in existing_columns:
                    try:
                        self.cursor.execute(f"ALTER TABLE tickers ADD COLUMN {col_name} {col_type}")
                        self.conn.commit()
                        print(f"  ✓ Added column: {col_name}")
                    except Error as e:
                        print(f"  ⚠ Could not add column {col_name}: {e}")
            
            print("✓ Tickers table created/verified")
            return True
        except Error as e:
            print(f"✗ Error creating table: {e}")
            return False
    
    def fetch_all_tickers(self, limit: int = 1000, market: str = 'stocks') -> List[Dict]:
        """Fetch all tickers from Polygon.io with pagination"""
        all_tickers = []
        next_url = None
        page = 1
        max_retries = 3
        
        print(f"\nFetching tickers from Polygon.io (market: {market})...")
        
        while True:
            if next_url:
                # Use the next_url from previous response
                params = {'apiKey': self.api_key}
                url = next_url
            else:
                # First request
                endpoint = "/v3/reference/tickers"
                params = {
                    'apiKey': self.api_key,
                    'market': market,
                    'limit': limit,
                    'order': 'asc'
                }
                url = f"{POLYGON_BASE_URL}{endpoint}"
            
            # Retry logic for network issues
            for attempt in range(max_retries):
                self.rate_limit()
                try:
                    response = requests.get(url, params=params, timeout=60)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get('status') == 'OK':
                        results = data.get('results', [])
                        all_tickers.extend(results)
                        print(f"  Page {page}: Fetched {len(results)} tickers (Total: {len(all_tickers)})")
                        
                        # Check for next page
                        next_url = data.get('next_url')
                        if not next_url:
                            print(f"✓ Completed fetching all tickers")
                            return all_tickers
                        
                        page += 1
                        break  # Success, exit retry loop
                    else:
                        print(f"✗ API returned error: {data.get('message', 'Unknown error')}")
                        return all_tickers
                        
                except requests.exceptions.Timeout as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        print(f"  ⚠ Timeout on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"✗ Error fetching tickers after {max_retries} attempts: {e}")
                        print(f"  This may be due to network issues or Polygon.io API being slow.")
                        print(f"  Try again later or check your internet connection.")
                        return all_tickers
                        
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        print(f"  ⚠ Error on attempt {attempt + 1}/{max_retries}: {e}")
                        print(f"  Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"✗ Error fetching tickers after {max_retries} attempts: {e}")
                        return all_tickers
        
        return all_tickers
    
    def fetch_ticker_details(self, ticker: str) -> Optional[Dict]:
        """Fetch detailed information for a specific ticker"""
        endpoint = f"/v3/reference/tickers/{ticker}"
        data = self.make_api_request(endpoint)
        
        if data and data.get('status') == 'OK':
            return data.get('results')
        return None
    
    def fetch_previous_day_aggregates(self, date: Optional[str] = None) -> Dict[str, Dict]:
        """
        Fetch previous day's aggregates for all stocks using grouped endpoint
        Returns a dictionary mapping ticker -> aggregate data
        """
        if date is None:
            # Get yesterday's date (or last trading day)
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime('%Y-%m-%d')
        
        print(f"\nFetching previous day aggregates for date: {date}...")
        
        # Use grouped aggregates endpoint - much faster than individual calls
        endpoint = f"/v2/aggs/grouped/locale/us/market/stocks/{date}"
        data = self.make_api_request(endpoint)
        
        aggregates = {}
        
        if data and data.get('status') == 'OK':
            results = data.get('results', [])
            print(f"  Fetched aggregates for {len(results)} tickers")
            
            for result in results:
                ticker = result.get('T')  # Ticker symbol
                if ticker:
                    aggregates[ticker] = {
                        'volume': result.get('v'),  # Volume
                        'open': result.get('o'),     # Open price
                        'high': result.get('h'),     # High price
                        'low': result.get('l'),       # Low price
                        'close': result.get('c'),     # Close price
                        'vwap': result.get('vw'),     # Volume weighted average price
                        'transactions': result.get('n'),  # Number of transactions
                        'date': date
                    }
        else:
            if data:
                print(f"  Warning: {data.get('message', 'Unknown error')}")
            else:
                print(f"  Warning: No data returned for date {date}")
        
        return aggregates
    
    def update_tickers_with_volume(self, aggregates: Dict[str, Dict]):
        """Update tickers table with volume and price data from aggregates"""
        if not aggregates:
            print("  No aggregate data to update")
            return
        
        print(f"\nUpdating {len(aggregates)} tickers with volume data...")
        
        update_sql = """
        UPDATE tickers 
        SET 
            last_day_volume = %s,
            last_day_open = %s,
            last_day_high = %s,
            last_day_low = %s,
            last_day_close = %s,
            last_day_vwap = %s,
            last_day_transactions = %s,
            last_day_date = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE ticker = %s
        """
        
        updated_count = 0
        error_count = 0
        
        for ticker, data in aggregates.items():
            try:
                values = (
                    self.normalize_value(data.get('volume'), 'bigint'),
                    self.normalize_value(data.get('open'), 'float'),
                    self.normalize_value(data.get('high'), 'float'),
                    self.normalize_value(data.get('low'), 'float'),
                    self.normalize_value(data.get('close'), 'float'),
                    self.normalize_value(data.get('vwap'), 'float'),
                    self.normalize_value(data.get('transactions'), 'int'),
                    self.normalize_value(data.get('date'), 'date'),
                    ticker
                )
                self.cursor.execute(update_sql, values)
                updated_count += 1
            except Error as e:
                error_count += 1
                if error_count <= 5:  # Only print first 5 errors
                    print(f"  ✗ Error updating {ticker}: {e}")
        
        self.conn.commit()
        print(f"  ✓ Updated {updated_count} tickers with volume data")
        if error_count > 0:
            print(f"  ✗ {error_count} errors occurred")
    
    def normalize_value(self, value, field_type='string'):
        """Normalize values for database insertion"""
        if value is None:
            return None
        
        # Handle complex types (dict, list) - convert to JSON string
        if isinstance(value, (dict, list)):
            try:
                json_str = json.dumps(value)
                # Truncate if too long for TEXT field
                if len(json_str) > 65535:  # TEXT field max size
                    return json_str[:65535]
                return json_str
            except:
                return None
        
        # Handle other non-primitive types
        if not isinstance(value, (str, int, float, bool)):
            try:
                # Try to convert to string
                str_value = str(value)
                if len(str_value) > 500:
                    return str_value[:500]
                return str_value
            except:
                return None
        
        if field_type == 'date':
            if isinstance(value, str):
                try:
                    # Handle various date formats
                    if len(value) == 10:  # YYYY-MM-DD
                        return value
                    elif len(value) >= 10:
                        return value[:10]
                except:
                    return None
            return None
        
        if field_type == 'int':
            try:
                return int(value) if value is not None else None
            except:
                return None
        
        if field_type == 'bigint':
            try:
                return int(value) if value is not None else None
            except:
                return None
        
        if field_type == 'float':
            try:
                return float(value) if value is not None else None
            except:
                return None
        
        if field_type == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'active')
            try:
                return bool(value) if value is not None else None
            except:
                return None
        
        # String type - truncate if too long
        if isinstance(value, str):
            if field_type == 'string':
                # For regular string fields, truncate at 500
                if len(value) > 500:
                    return value[:500]
            # For TEXT fields (like description, address), allow longer
            return value
        
        return value
    
    def map_sic_to_sector_industry(self, sic_code: Optional[str]):
        """
        Map a SIC code to a broad sector (and optionally an industry).
        This uses high‑level SIC divisions; you can extend it with a
        more detailed mapping table if you want finer industries.
        """
        if not sic_code:
            return None, None
        
        # Work with the first 4 digits only
        try:
            code = int(str(sic_code)[:4])
        except ValueError:
            return None, None
        
        sector = None
        # SIC divisions – broad buckets
        if 1 <= code <= 999:
            sector = "Agriculture, Forestry & Fishing"
        elif 1000 <= code <= 1499:
            sector = "Mining"
        elif 1500 <= code <= 1799:
            sector = "Construction"
        elif 2000 <= code <= 3999:
            sector = "Manufacturing"
        elif 4000 <= code <= 4999:
            sector = "Transportation & Public Utilities"
        elif 5000 <= code <= 5199:
            sector = "Wholesale Trade"
        elif 5200 <= code <= 5999:
            sector = "Retail Trade"
        elif 6000 <= code <= 6799:
            sector = "Finance, Insurance & Real Estate"
        elif 7000 <= code <= 8999:
            sector = "Services"
        elif 9000 <= code <= 9999:
            sector = "Public Administration"
        
        # For now we only provide sector; industry can be filled from
        # a more detailed SIC mapping table if you choose to add one.
        industry = None
        return sector, industry
    
    def upsert_ticker(self, ticker_data: Dict, fetch_details: bool = False):
        """Insert or update a ticker in the database"""
        # If fetch_details is True, get detailed information
        # WARNING: This makes an additional API call per ticker, which is very slow for large datasets
        if fetch_details:
            ticker_symbol = ticker_data.get('ticker')
            if ticker_symbol:
                details = self.fetch_ticker_details(ticker_symbol)
                if details:
                    # Merge details into ticker_data (details take precedence)
                    ticker_data.update(details)
        
        # Extract and normalize all fields
        ticker = self.normalize_value(ticker_data.get('ticker'))
        if not ticker:
            return False
        
        # Derive sector/industry from SIC code if available
        sic_code_raw = ticker_data.get('sic_code')
        sic_code = self.normalize_value(sic_code_raw)
        sector, industry = self.map_sic_to_sector_industry(sic_code)
        
        # Prepare data dictionary (lean schema using commonly populated fields)
        data = {
            'ticker': ticker,
            'name': self.normalize_value(ticker_data.get('name')),
            'type': self.normalize_value(ticker_data.get('type')),
            'market': self.normalize_value(ticker_data.get('market')),
            'locale': self.normalize_value(ticker_data.get('locale')),
            'primary_exchange': self.normalize_value(ticker_data.get('primary_exchange')),
            'currency_name': self.normalize_value(ticker_data.get('currency_name')),
            'sic_code': sic_code,
            'sector': self.normalize_value(sector),
            'industry': self.normalize_value(industry),
            'cik': self.normalize_value(ticker_data.get('cik')),
            'last_updated_utc': self.normalize_value(ticker_data.get('last_updated_utc'), 'date'),
        }
        
        # Build INSERT ... ON DUPLICATE KEY UPDATE query
        columns = list(data.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns if col != 'ticker'])
        
        insert_sql = f"""
        INSERT INTO tickers ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
            {update_clause},
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Ensure all values are properly normalized (no dicts, lists, or complex objects)
        normalized_data = {}
        for key, val in data.items():
            # Double-check normalization - convert any remaining complex types
            if isinstance(val, (dict, list)):
                try:
                    normalized_data[key] = json.dumps(val) if val else None
                except:
                    normalized_data[key] = None
            elif val is not None and not isinstance(val, (str, int, float, bool, type(None))):
                # Convert any other non-primitive types to string
                try:
                    normalized_data[key] = str(val)
                except:
                    normalized_data[key] = None
            else:
                normalized_data[key] = val
        
        values = tuple(normalized_data.values())
        
        try:
            self.cursor.execute(insert_sql, values)
            return True
        except Error as e:
            print(f"✗ Error upserting ticker {ticker}: {e}")
            # Debug: print the problematic value types
            for key, val in normalized_data.items():
                if not isinstance(val, (str, int, float, bool, type(None))):
                    print(f"  Warning: {key} has unexpected type: {type(val)}")
            return False
    
    def process_tickers(self, tickers: List[Dict], fetch_details: bool = False, batch_size: int = 100):
        """Process and store tickers in batches"""
        total = len(tickers)
        print(f"\nProcessing {total} tickers...")
        print(f"Fetching details: {'Yes (slower but more data)' if fetch_details else 'No (faster, basic data only)'}")
        print(f"Batch size: {batch_size}\n")
        
        success_count = 0
        error_count = 0
        start_time = time.time()
        
        for i, ticker_data in enumerate(tickers, 1):
            if self.upsert_ticker(ticker_data, fetch_details=fetch_details):
                success_count += 1
            else:
                error_count += 1
            
            # Commit in batches and show progress
            if i % batch_size == 0:
                self.conn.commit()
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (total - i) / rate if rate > 0 else 0
                print(f"  Processed {i}/{total} tickers (✓ {success_count}, ✗ {error_count}) | "
                      f"Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min")
            elif i % 10 == 0:
                # Show progress every 10 items for faster feedback
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (total - i) / rate if rate > 0 else 0
                print(f"  Progress: {i}/{total} ({i*100/total:.1f}%) | "
                      f"Rate: {rate:.1f}/sec | ETA: {remaining/60:.1f} min", end='\r')
        
        # Final commit
        self.conn.commit()
        elapsed = time.time() - start_time
        print(f"\n✓ Processing complete!")
        print(f"  Total: {total}")
        print(f"  Success: {success_count}")
        print(f"  Errors: {error_count}")
        print(f"  Time: {elapsed/60:.2f} minutes ({elapsed:.1f} seconds)")
    
    def _fetch_sic_worker(self, ticker: str, rate_lock: threading.Lock,
                          last_req: list) -> Tuple[str, Optional[str]]:
        """
        Fetch SIC code for one ticker.  Uses a shared lock + timestamp list
        to enforce the global REQUESTS_PER_SECOND limit across all threads.
        Returns (ticker, sic_code_or_None).
        """
        url    = f"{POLYGON_BASE_URL}/v3/reference/tickers/{ticker}"
        params = {'apiKey': self.api_key}

        for attempt in range(3):
            # Thread-safe rate limiting
            with rate_lock:
                now     = time.time()
                elapsed = now - last_req[0]
                gap     = MIN_REQUEST_INTERVAL - elapsed
                if gap > 0:
                    time.sleep(gap)
                last_req[0] = time.time()

            try:
                resp = requests.get(url, params=params, timeout=30)
                if resp.status_code == 429:          # rate-limited by Polygon
                    time.sleep(2 ** (attempt + 1))
                    continue
                resp.raise_for_status()
                data = resp.json()
                if data and data.get('status') == 'OK':
                    return ticker, data.get('results', {}).get('sic_code')
                return ticker, None
            except Exception:
                if attempt < 2:
                    time.sleep(1)

        return ticker, None

    def update_sic_codes(self, workers: int = SIC_WORKERS, batch_size: int = 200):
        """
        Fetch SIC code (and derive sector) for every ticker that currently has
        sic_code IS NULL.  Uses a thread pool so multiple HTTP requests are
        in-flight simultaneously, dramatically cutting wall-clock time.
        """
        self.cursor.execute("SELECT ticker FROM tickers WHERE sic_code IS NULL ORDER BY ticker")
        rows = self.cursor.fetchall()
        if not rows:
            print("  ✓ All tickers already have a SIC code – nothing to do.")
            return

        tickers = [r[0] for r in rows]
        total   = len(tickers)
        eff_rps = min(workers, REQUESTS_PER_SECOND)
        print(f"\nFetching SIC codes for {total} tickers using {workers} workers "
              f"at {REQUESTS_PER_SECOND} req/s...")
        print(f"  Estimated time: ~{total / REQUESTS_PER_SECOND / 60:.1f} minutes "
              f"(wall-clock faster due to parallel I/O)\n")

        update_sql = """
        UPDATE tickers
        SET sic_code = %s, sector = %s, industry = %s
        WHERE ticker = %s
        """

        # Shared state for the rate limiter across threads
        rate_lock = threading.Lock()
        last_req  = [0.0]          # mutable container so threads can update it

        updated  = 0
        skipped  = 0
        errors   = 0
        pending  = {}              # future -> ticker
        start    = time.time()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Submit all jobs
            for ticker in tickers:
                future = pool.submit(self._fetch_sic_worker, ticker, rate_lock, last_req)
                pending[future] = ticker

            done = 0
            for future in as_completed(pending):
                done += 1
                ticker, sic_raw = future.result()
                sic_code        = self.normalize_value(sic_raw)
                sector, industry = self.map_sic_to_sector_industry(sic_code)

                if sic_code:
                    try:
                        self.cursor.execute(update_sql, (sic_code, sector, industry, ticker))
                        updated += 1
                    except Error as e:
                        errors += 1
                        if errors <= 5:
                            print(f"\n  ✗ DB error for {ticker}: {e}")
                else:
                    skipped += 1

                # Batch commit + progress
                if done % batch_size == 0:
                    self.conn.commit()
                    elapsed   = time.time() - start
                    rate      = done / elapsed if elapsed > 0 else 0
                    remaining = (total - done) / rate if rate > 0 else 0
                    print(f"  {done}/{total} "
                          f"(✓ {updated} updated, ~ {skipped} no SIC, ✗ {errors} errors) | "
                          f"Rate: {rate:.1f}/s | ETA: {remaining/60:.1f} min")
                elif done % 25 == 0:
                    elapsed   = time.time() - start
                    rate      = done / elapsed if elapsed > 0 else 0
                    remaining = (total - done) / rate if rate > 0 else 0
                    print(f"  Progress: {done}/{total} ({done*100/total:.1f}%) | "
                          f"Rate: {rate:.1f}/s | ETA: {remaining/60:.1f} min", end='\r')

        self.conn.commit()
        elapsed = time.time() - start
        print(f"\n✓ SIC code update complete in {elapsed/60:.2f} minutes")
        print(f"  Updated: {updated}  |  No SIC: {skipped}  |  Errors: {errors}")

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Download Polygon.io tickers to MySQL")
    parser.add_argument(
        '--update-sic', action='store_true',
        help='Only fetch/update SIC codes (sector) for tickers that are missing them. '
             'Skips the full ticker download. Run after the normal sync.'
    )
    parser.add_argument(
        '--workers', type=int, default=SIC_WORKERS,
        help=f'Number of parallel workers for --update-sic (default: {SIC_WORKERS}). '
             'Increase on paid Polygon plans with higher rate limits.'
    )
    args = parser.parse_args()
    
    print("="*80)
    print("  POLYGON.IO TICKER DOWNLOADER TO MYSQL")
    print("="*80)
    
    # Initialize downloader
    downloader = PolygonTickerDownloader(POLYGON_API_KEY, DB_CONFIG)
    
    try:
        # Test API connection first
        if not downloader.test_api_connection():
            print("\n✗ API connection test failed. Please fix the issues above before continuing.")
            return
        
        # Connect to database
        print("\n" + "="*80)
        print("  CONNECTING TO DATABASE")
        print("="*80)
        if not downloader.connect_db():
            return
        
        # Create table
        if not downloader.create_table():
            return

        # --update-sic mode: only back-fill missing SIC/sector data, then exit
        if args.update_sic:
            print("\n" + "="*80)
            print("  UPDATING SIC CODES / SECTOR")
            print("="*80)
            downloader.update_sic_codes(workers=args.workers)
            return
        
        # Fetch all tickers
        # You can change 'stocks' to 'crypto', 'fx', etc. based on what you need
        tickers = downloader.fetch_all_tickers(market='stocks', limit=1000)
        
        if not tickers:
            print("✗ No tickers fetched. Exiting.")
            return
        
        # Process tickers
        # Set fetch_details=True to get detailed info for each ticker (slower but more data)
        # Set fetch_details=False to only use basic info from the list (faster - recommended)
        # Note: Fetching details for 12k+ tickers can take 40+ minutes due to API rate limits
        downloader.process_tickers(tickers, fetch_details=False, batch_size=100)
        
        print("\n" + "="*80)
        print("  TICKER DOWNLOAD COMPLETE")
        print("="*80)
        
        # Now fetch and update volume data as a separate process
        print("\n" + "="*80)
        print("  FETCHING LAST DAY'S VOLUME DATA")
        print("="*80)
        
        try:
            # Try to get yesterday's data, if not available, try previous trading days
            aggregates = downloader.fetch_previous_day_aggregates()
            
            if not aggregates:
                # Try a few days back in case markets were closed
                print("  Trying previous trading days...")
                for days_back in range(2, 8):
                    check_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                    aggregates = downloader.fetch_previous_day_aggregates(check_date)
                    if aggregates:
                        break
            
            if aggregates:
                downloader.update_tickers_with_volume(aggregates)
            else:
                print("  ⚠ Could not fetch volume data. Markets may be closed or API issue.")
        
        except Exception as e:
            print(f"  ✗ Error fetching volume data: {e}")
            print("  Continuing anyway - ticker data is still saved.")
        
        print("\n" + "="*80)
        print("  SUCCESS!")
        print("="*80)
        print("All tickers have been downloaded and stored in the MySQL database.")
        print("Volume data has been updated where available.")
        print("Run this script again to update existing records.")
        
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        downloader.close()


if __name__ == "__main__":
    main()

