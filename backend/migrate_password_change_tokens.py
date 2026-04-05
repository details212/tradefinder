"""
Migration: create the password_change_tokens table.
Run once:  py migrate_password_change_tokens.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

DDL = """
CREATE TABLE IF NOT EXISTS password_change_tokens (
    id                 INT AUTO_INCREMENT PRIMARY KEY,
    user_id            INT          NOT NULL,
    new_password_hash  VARCHAR(256) NOT NULL,
    code               VARCHAR(6)   NOT NULL,
    expires_at         DATETIME     NOT NULL,
    created_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pct_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    UNIQUE INDEX uq_pct_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

app = create_app()
with app.app_context():
    db.session.execute(db.text(DDL))
    db.session.commit()
    print("password_change_tokens table created (or already exists).")
