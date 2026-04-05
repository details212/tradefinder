"""
Migration: create the email_verifications table.
Run once:  python migrate_email_verifications.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

DDL = """
CREATE TABLE IF NOT EXISTS email_verifications (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    user_id    INT          NOT NULL,
    new_email  VARCHAR(120) NOT NULL,
    code       VARCHAR(6)   NOT NULL,
    expires_at DATETIME     NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ev_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    UNIQUE INDEX uq_ev_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

app = create_app()
with app.app_context():
    db.session.execute(db.text(DDL))
    db.session.commit()
    print("email_verifications table created (or already exists).")
