from flask import Blueprint, request, jsonify
from auth import token_required
from models import Setting

settings_bp = Blueprint("settings", __name__, url_prefix="/api/settings")

# Keys whose values are masked in the generic list / update responses
SENSITIVE_KEYS = {
    "alpaca_api_key",
    "alpaca_api_secret",
}

# Keys excluded from the generic list endpoint entirely (managed via .env / config)
HIDDEN_KEYS = {
    "polygon_api_key",
}


def mask(key: str, value: str) -> str:
    if key in SENSITIVE_KEYS and value:
        return value[:6] + "****" + value[-4:]
    return value


@settings_bp.route("/", methods=["GET"])
@token_required
def get_settings(current_user):
    rows = Setting.query.order_by(Setting.key).all()
    return jsonify({
        "settings": [
            {**r.to_dict(), "value": mask(r.key, r.value)}
            for r in rows
            if r.key not in HIDDEN_KEYS
        ]
    }), 200


@settings_bp.route("/<string:key>", methods=["PUT"])
@token_required
def update_setting(current_user, key: str):
    data = request.get_json(silent=True) or {}
    value = data.get("value")
    description = data.get("description")

    if key in HIDDEN_KEYS:
        return jsonify({"error": "This setting is not configurable via the API."}), 403

    if value is None:
        return jsonify({"error": "value is required"}), 400

    row = Setting.set(key, str(value), description)
    return jsonify({
        "setting": {**row.to_dict(), "value": mask(key, row.value)}
    }), 200
