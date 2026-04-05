import jwt
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import request, jsonify, current_app


def generate_token(user_id: int) -> str:
    expiry = datetime.now(timezone.utc) + timedelta(
        hours=current_app.config["JWT_EXPIRATION_HOURS"]
    )
    payload = {
        "sub": user_id,
        "iat": datetime.now(timezone.utc),
        "exp": expiry,
    }
    return jwt.encode(payload, current_app.config["JWT_SECRET_KEY"], algorithm="HS256")


def decode_token(token: str) -> dict:
    return jwt.decode(
        token,
        current_app.config["JWT_SECRET_KEY"],
        algorithms=["HS256"],
    )


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401

        token = auth_header.split(" ", 1)[1]
        try:
            payload = decode_token(token)
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

        from models import User
        user = User.query.get(payload["sub"])
        if not user or not user.is_active:
            return jsonify({"error": "User not found or inactive"}), 401

        return f(user, *args, **kwargs)

    return decorated
