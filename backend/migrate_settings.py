"""
Run once (or re-run safely) to create all tables and seed global defaults.
Usage: python migrate_settings.py
"""
from app import create_app
from extensions import db
from models import Setting

app = create_app()

with app.app_context():
    # Creates any missing tables, including the new broker_credentials table
    db.create_all()

    # ── Global app settings ───────────────────────────────────────────────────
    Setting.set(
        "polygon_api_key",
        "pntJnvnXxV3q2nAIdsph4RbT0b_oUlPE",
        "Polygon.io API key for market data",
    )

    print("Tables ready.")
    print("Global settings seeded.")
