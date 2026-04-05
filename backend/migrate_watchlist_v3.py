"""
Migration: add bar_time column to the watchlist table.
Run once: python migrate_watchlist_v3.py
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

cursor.execute(
    "SELECT COUNT(*) FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'watchlist' AND COLUMN_NAME = 'bar_time'"
)
if cursor.fetchone()[0] == 0:
    cursor.execute(
        "ALTER TABLE watchlist ADD COLUMN bar_time DATETIME NULL "
        "COMMENT 'Strategy signal bar time (ET)'"
    )
    conn.commit()
    print("Added column: bar_time")
else:
    print("Already exists: bar_time")

cursor.close()
conn.close()
print("Done.")
