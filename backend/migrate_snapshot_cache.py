"""
Migration: create snapshot_cache table.
Run once: python migrate_snapshot_cache.py
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

cursor.execute("""
    CREATE TABLE IF NOT EXISTS snapshot_cache (
        ticker      VARCHAR(20)    NOT NULL PRIMARY KEY,
        price       DECIMAL(18,4)  NULL,
        change_pct  DECIMAL(10,4)  NULL,
        updated_at  DATETIME       NULL,
        INDEX idx_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
conn.commit()
cursor.close()
conn.close()
print("snapshot_cache table ready.")
