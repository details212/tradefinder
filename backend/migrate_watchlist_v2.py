"""
Migration: add bias and threshold columns to the watchlist table.
Run once: python migrate_watchlist_v2.py
"""
import pymysql
from urllib.parse import urlparse
from config import Config

url = urlparse(Config.SQLALCHEMY_DATABASE_URI.replace("mysql+pymysql://", "mysql://"))
conn = pymysql.connect(
    host=url.hostname,
    port=url.port or 3306,
    user=url.username,
    password=url.password,
    database=url.path.lstrip("/"),
)
cursor = conn.cursor()

migrations = [
    ("bias",      "ALTER TABLE watchlist ADD COLUMN bias      VARCHAR(20)    NULL COMMENT 'Long / Short / etc.'"),
    ("threshold", "ALTER TABLE watchlist ADD COLUMN threshold DECIMAL(18,4)  NULL COMMENT 'Key price level, e.g. resistance'"),
]

for col, sql in migrations:
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'watchlist' AND COLUMN_NAME = %s",
        (col,)
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(sql)
        print(f"  Added column: {col}")
    else:
        print(f"  Already exists: {col}")

conn.commit()
cursor.close()
conn.close()
print("Done.")
