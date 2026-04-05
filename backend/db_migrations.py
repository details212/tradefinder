"""
Idempotent schema patches for existing MySQL/SQLite DBs.

Flask-SQLAlchemy `create_all()` creates missing tables but does not ALTER
existing tables when new columns are added to models.
"""

from sqlalchemy import text
from sqlalchemy.exc import OperationalError


def apply_runtime_migrations(db) -> None:
    """Call once after `db.create_all()` inside app context."""
    _ensure_watchlist_source_column(db)


def _ensure_watchlist_source_column(db) -> None:
    try:
        db.session.execute(
            text("ALTER TABLE watchlist ADD COLUMN source VARCHAR(32) NULL")
        )
        db.session.commit()
    except OperationalError as e:
        db.session.rollback()
        orig = getattr(e, "orig", None)
        code = orig.args[0] if orig and getattr(orig, "args", None) else None
        msg = str(e).lower()
        # MySQL 1060 = ER_DUP_FIELDNAME; SQLite/others: message text
        if code == 1060 or "duplicate column" in msg:
            return
        raise
