"""
Run this script once to create an initial admin user.
Usage: python seed.py
"""
import pymysql
from config import Config
from urllib.parse import urlparse

def ensure_database_exists():
    """Create the MySQL database if it doesn't already exist."""
    uri = Config.SQLALCHEMY_DATABASE_URI
    # Parse the URI: mysql+pymysql://user:pass@host/dbname
    parsed = urlparse(uri.replace("mysql+pymysql://", "mysql://"))
    host = parsed.hostname
    port = parsed.port or 3306
    user = parsed.username
    password = parsed.password
    dbname = parsed.path.lstrip("/")

    conn = pymysql.connect(host=host, port=port, user=user, password=password)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{dbname}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
        print(f"Database '{dbname}' is ready.")
    finally:
        conn.close()

ensure_database_exists()

from app import create_app
from extensions import db
from models import User

app = create_app()

with app.app_context():
    db.create_all()

    if not User.query.filter_by(username="admin").first():
        admin = User(username="admin", email="admin@tradefinder.local")
        admin.set_password("changeme123")
        db.session.add(admin)
        db.session.commit()
        print("Created admin user — username: admin / password: changeme123")
    else:
        print("Admin user already exists.")
