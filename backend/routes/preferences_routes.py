"""
Per-user preferences — stored via BrokerCredential with broker="preferences".

GET  /api/preferences/   → { preferences: { key: value, … } }
PUT  /api/preferences/   → body { key: value, … }  (upserts each pair)

Common keys:
  risk_mode, risk_value — risk management UI
  auto_market_close_beyond_tp — "true"|"false"; server may flatten when snapshot price is past TP
"""

from flask import Blueprint, request, jsonify
from auth import token_required
from models import BrokerCredential

preferences_bp = Blueprint("preferences", __name__, url_prefix="/api/preferences")

BROKER = "preferences"


@preferences_bp.route("/", methods=["GET"])
@token_required
def get_preferences(current_user):
    prefs = BrokerCredential.get_all(current_user.id, BROKER)
    return jsonify({"preferences": prefs}), 200


@preferences_bp.route("/", methods=["PUT"])
@token_required
def update_preferences(current_user):
    data = request.get_json(silent=True) or {}
    if not data:
        return jsonify({"error": "No data provided."}), 400
    for key, value in data.items():
        BrokerCredential.set(current_user.id, BROKER, key, str(value))
    return jsonify({"ok": True}), 200
