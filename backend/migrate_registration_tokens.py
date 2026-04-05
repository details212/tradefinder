"""
Migration: create the registration_tokens table.
Run once:  py migrate_registration_tokens.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

DDL = """
CREATE TABLE IF NOT EXISTS registration_tokens (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    email      VARCHAR(120) NOT NULL,
    code       VARCHAR(6)   NOT NULL,
    verified   TINYINT(1)   NOT NULL DEFAULT 0,
    expires_at DATETIME     NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX uq_rt_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

app = create_app()
with app.app_context():
    db.session.execute(db.text(DDL))
    db.session.commit()
    print("registration_tokens table created (or already exists).")
