"""
Migration: add profile columns to the users table.
Run once:  py migrate_users_profile.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

COLUMNS = [
    ("first_name",   "VARCHAR(80)  NULL"),
    ("last_name",    "VARCHAR(80)  NULL"),
    ("address",      "VARCHAR(255) NULL"),
    ("city",         "VARCHAR(100) NULL"),
    ("state",        "VARCHAR(2)   NULL"),
    ("zipcode",      "VARCHAR(10)  NULL"),
    ("mobile_phone", "VARCHAR(15)  NULL"),
]

app = create_app()
with app.app_context():
    # Fetch existing columns once
    existing = {
        row[0]
        for row in db.session.execute(
            db.text("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'")
        )
    }
    for col, definition in COLUMNS:
        if col in existing:
            print(f"SKIP (already exists): {col}")
        else:
            db.session.execute(db.text(f"ALTER TABLE users ADD COLUMN {col} {definition}"))
            db.session.commit()
            print(f"ADDED: {col}")
    print("Done.")
