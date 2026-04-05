import secrets
import re
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify
import requests as http_req
from extensions import db
from models import User, Order, LoginEvent, EmailVerification, PasswordChangeToken, RegistrationToken, Setting
from auth import generate_token, token_required

# ── Postmark helper ───────────────────────────────────────────────────────────
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

def _send_verification_email(to_email: str, code: str, subject: str = None) -> None:
    """Send a 6-digit verification code via Postmark."""
    token  = Setting.get("postmark_api_token", "")
    sender = Setting.get("postmark_email", "")
    if not token or not sender:
        raise RuntimeError("Postmark is not configured (missing postmark_api_token or postmark_email in settings).")

    subject = subject or "TradeFinder — your verification code"
    year    = datetime.now().year

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{subject}</title>
</head>
<body style="margin:0;padding:0;background:#0b1120;font-family:'Segoe UI',Arial,sans-serif;">

  <!-- Wrapper -->
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background:#0b1120;padding:40px 16px;">
    <tr><td align="center">

      <!-- Card -->
      <table width="560" cellpadding="0" cellspacing="0" border="0"
             style="max-width:560px;width:100%;background:#0f172a;
                    border:1px solid #1e293b;border-radius:12px;overflow:hidden;">

        <!-- Header bar -->
        <tr>
          <td style="background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 100%);
                     padding:28px 36px;">
            <p style="margin:0;font-size:22px;font-weight:700;color:#ffffff;
                      letter-spacing:-0.3px;">🔒 TradeFinder</p>
            <p style="margin:4px 0 0;font-size:13px;color:#c4b5fd;">
              Security Verification
            </p>
          </td>
        </tr>

        <!-- Body -->
        <tr>
          <td style="padding:36px 36px 24px;">

            <p style="margin:0 0 6px;font-size:18px;font-weight:600;color:#f1f5f9;">
              Your verification code
            </p>
            <p style="margin:0 0 24px;font-size:14px;color:#94a3b8;line-height:1.6;">
              Use the one-time code below to complete your request.
              For your security, this code expires in
              <strong style="color:#e2e8f0;">15 minutes</strong> and
              can only be used once.
            </p>

            <!-- Code box -->
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td align="center"
                    style="background:#1e293b;border:1px solid #334155;
                           border-radius:10px;padding:24px 16px;">
                  <p style="margin:0;font-size:40px;font-weight:800;
                            letter-spacing:0.35em;color:#ffffff;
                            font-family:'Courier New',monospace;">{code}</p>
                </td>
              </tr>
            </table>

            <!-- What to do -->
            <p style="margin:28px 0 6px;font-size:14px;font-weight:600;color:#e2e8f0;">
              What to do next
            </p>
            <ol style="margin:0;padding-left:20px;color:#94a3b8;
                       font-size:13px;line-height:1.8;">
              <li>Return to the TradeFinder application.</li>
              <li>Enter the 6-digit code in the verification field.</li>
              <li>Complete your request — the code is valid for one use only.</li>
            </ol>

            <!-- Security notice -->
            <table width="100%" cellpadding="0" cellspacing="0" border="0"
                   style="margin-top:28px;">
              <tr>
                <td style="background:#172033;border-left:3px solid #4f46e5;
                           border-radius:0 6px 6px 0;padding:14px 16px;">
                  <p style="margin:0;font-size:13px;color:#94a3b8;line-height:1.6;">
                    <strong style="color:#c4b5fd;">Did not request this?</strong><br/>
                    If you did not make this request, you can safely ignore this email.
                    No changes will be made to your account. Your TradeFinder credentials
                    remain secure. If you believe someone else is attempting to access
                    your account, please contact us immediately at
                    <a href="mailto:{sender}" style="color:#818cf8;">{sender}</a>.
                  </p>
                </td>
              </tr>
            </table>

          </td>
        </tr>

        <!-- Divider -->
        <tr>
          <td style="padding:0 36px;">
            <hr style="border:none;border-top:1px solid #1e293b;margin:0;" />
          </td>
        </tr>

        <!-- About section -->
        <tr>
          <td style="padding:20px 36px 8px;">
            <p style="margin:0 0 8px;font-size:13px;font-weight:600;color:#64748b;
                      text-transform:uppercase;letter-spacing:0.05em;">
              About TradeFinder
            </p>
            <p style="margin:0;font-size:13px;color:#64748b;line-height:1.7;">
              TradeFinder is a real-time stock market trade-idea platform that surfaces
              high-probability setups using technical analysis, moving-average studies,
              and live market data.  We combine rule-based scanning with Alpaca brokerage
              integration so you can research, plan, and execute trades from a single
              interface — all backed by Polygon.io market data.
            </p>
          </td>
        </tr>

        <!-- Footer -->
        <tr>
          <td style="padding:20px 36px 32px;">
            <p style="margin:0 0 4px;font-size:12px;color:#475569;line-height:1.6;">
              You received this email because a verification code was requested for the
              TradeFinder account associated with
              <strong style="color:#64748b;">{to_email}</strong>.
              This is a transactional security email — you cannot unsubscribe from
              account security notifications.
            </p>
            <p style="margin:12px 0 0;font-size:11px;color:#334155;">
              &copy; {year} TradeFinder &nbsp;&bull;&nbsp;
              <a href="mailto:{sender}" style="color:#475569;text-decoration:none;">{sender}</a>
            </p>
          </td>
        </tr>

      </table>
      <!-- /Card -->

    </td></tr>
  </table>
  <!-- /Wrapper -->

</body>
</html>"""

    text = (
        f"TradeFinder — Security Verification\n"
        f"{'=' * 40}\n\n"
        f"Your verification code is:\n\n"
        f"  {code}\n\n"
        f"This code expires in 15 minutes and can only be used once.\n\n"
        f"Steps:\n"
        f"  1. Return to the TradeFinder application.\n"
        f"  2. Enter the 6-digit code in the verification field.\n"
        f"  3. Complete your request.\n\n"
        f"Did not request this? Ignore this email — no changes will be made.\n"
        f"Concerned about unauthorised access? Email us at {sender}.\n\n"
        f"---\n"
        f"TradeFinder is a real-time stock market trade-idea platform providing\n"
        f"technical analysis, moving-average studies, and live brokerage integration.\n\n"
        f"You received this email because a code was requested for {to_email}.\n"
        f"This is a transactional security email.\n"
        f"© {year} TradeFinder · {sender}\n"
    )

    http_req.post(
        "https://api.postmarkapp.com/email",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Postmark-Server-Token": token,
        },
        json={
            "From":          f"TradeFinder <{sender}>",
            "To":            to_email,
            "Subject":       subject,
            "HtmlBody":      html,
            "TextBody":      text,
            "MessageStream": "outbound",
        },
        timeout=10,
    ).raise_for_status()

auth_bp = Blueprint("auth", __name__, url_prefix="/api/auth")

# Statuses Alpaca considers permanently closed (mirrors alpaca_routes.py)
_CLOSED_STATUSES = {"canceled", "expired", "rejected", "done_for_day"}


def _parse_platform(ua: str) -> str:
    """Return a short human-readable platform string from a User-Agent header."""
    if not ua:
        return "Unknown"
    ua_lower = ua.lower()
    # Electron app
    if "electron" in ua_lower:
        return "Electron app"
    # Mobile
    if "iphone" in ua_lower or "ipad" in ua_lower:
        return "iOS"
    if "android" in ua_lower:
        return "Android"
    # OS
    if "windows nt" in ua_lower:
        os_name = "Windows"
    elif "macintosh" in ua_lower or "mac os" in ua_lower:
        os_name = "macOS"
    elif "linux" in ua_lower:
        os_name = "Linux"
    else:
        os_name = "Unknown OS"
    # Browser
    if "edg/" in ua_lower:
        browser = "Edge"
    elif "chrome" in ua_lower:
        browser = "Chrome"
    elif "firefox" in ua_lower:
        browser = "Firefox"
    elif "safari" in ua_lower:
        browser = "Safari"
    else:
        browser = "Browser"
    return f"{browser} on {os_name}"


def _build_digest(user_id: int) -> dict:
    """Compute an account snapshot for the given user from the local DB."""
    orders = Order.query.filter_by(user_id=user_id).all()

    # Use the Alpaca-authoritative is_open flag written by the sync endpoint.
    # Fall back to status-based logic for orders not yet synced (is_open IS NULL).
    open_orders   = [o for o in orders if (
        o.is_open is True or
        (o.is_open is None and o.status and o.status not in _CLOSED_STATUSES)
    )]
    closed_orders = [
        o for o in orders
        if o.is_open is False and o.unrealized_pl is not None
    ]

    unrealized_pl = sum(float(o.unrealized_pl) for o in open_orders if o.unrealized_pl is not None)
    net_pl        = sum(float(o.unrealized_pl) for o in closed_orders)
    win_count     = sum(1 for o in closed_orders if float(o.unrealized_pl) > 0)
    loss_count    = sum(1 for o in closed_orders if float(o.unrealized_pl) < 0)

    return {
        "open_trades":  len(open_orders),
        "total_trades": len(orders),
        "unrealized_pl": unrealized_pl,
        "net_pl":        net_pl,
        "win_count":     win_count,
        "loss_count":    loss_count,
    }


@auth_bp.route("/send-registration-code", methods=["POST"])
def send_registration_code():
    """Send a verification PIN to an email before account creation."""
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()

    if not email:
        return jsonify({"error": "Email is required"}), 400
    if not _EMAIL_RE.match(email):
        return jsonify({"error": "Invalid email address"}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "An account with this email already exists"}), 409

    code       = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    rt = RegistrationToken.query.filter_by(email=email).first()
    if rt:
        rt.code       = code
        rt.verified   = False
        rt.expires_at = expires_at
        rt.created_at = datetime.now(timezone.utc)
    else:
        rt = RegistrationToken(email=email, code=code, expires_at=expires_at)
        db.session.add(rt)
    db.session.commit()

    try:
        _send_verification_email(email, code, subject="TradeFinder — confirm your email to create an account")
    except Exception as exc:
        db.session.delete(rt)
        db.session.commit()
        return jsonify({"error": f"Failed to send verification email: {exc}"}), 502

    return jsonify({"message": f"Verification code sent to {email}"}), 200


@auth_bp.route("/confirm-registration-code", methods=["POST"])
def confirm_registration_code():
    """Verify the PIN sent during registration. Marks the token as verified."""
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    code  = (data.get("code") or "").strip()

    if not email or not code:
        return jsonify({"error": "email and code are required"}), 400

    rt = RegistrationToken.query.filter_by(email=email).first()
    if not rt:
        return jsonify({"error": "No verification request found for this email"}), 404

    now = datetime.now(timezone.utc)
    if rt.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(rt)
        db.session.commit()
        return jsonify({"error": "Code has expired. Please request a new one."}), 410

    if not secrets.compare_digest(rt.code, code):
        return jsonify({"error": "Incorrect verification code"}), 400

    rt.verified = True
    db.session.commit()
    return jsonify({"ok": True}), 200


@auth_bp.route("/register", methods=["POST"])
def register():
    data     = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not username or not email or not password:
        return jsonify({"error": "username, email, and password are required"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400

    # Email must have been verified via the registration flow
    rt = RegistrationToken.query.filter_by(email=email).first()
    if not rt or not rt.verified:
        return jsonify({"error": "Email address has not been verified"}), 403

    now = datetime.now(timezone.utc)
    if rt.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(rt)
        db.session.commit()
        return jsonify({"error": "Verification has expired. Please start over."}), 410

    if User.query.filter((User.username == username) | (User.email == email)).first():
        return jsonify({"error": "Username or email already exists"}), 409

    user = User(username=username, email=email)
    user.set_password(password)

    # Optional profile fields sent during registration
    for field in ("first_name", "last_name", "address", "city", "state", "zipcode", "mobile_phone"):
        val = (data.get(field) or "").strip() or None
        setattr(user, field, val)

    db.session.add(user)

    # Clean up the used token
    db.session.delete(rt)
    db.session.commit()

    token = generate_token(user.id)
    return jsonify({"token": token, "user": user.to_dict()}), 201


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    user = User.query.filter(
        (User.username == username) | (User.email == username)
    ).first()

    if not user or not user.check_password(password):
        return jsonify({"error": "Invalid credentials"}), 401

    if not user.is_active:
        return jsonify({"error": "Account is disabled"}), 403

    # ── Update last_login ──────────────────────────────────────────────────────
    user.last_login = datetime.now(timezone.utc)

    # ── Build and persist login digest ────────────────────────────────────────
    try:
        ua = request.headers.get("User-Agent", "")
        # Respect X-Forwarded-For when behind a proxy; fall back to remote_addr
        ip = (request.headers.get("X-Forwarded-For", "") or "").split(",")[0].strip() \
             or request.remote_addr

        digest = _build_digest(user.id)

        event = LoginEvent(
            user_id      = user.id,
            logged_in_at = datetime.now(timezone.utc),
            ip_address   = ip,
            user_agent   = ua[:512] if ua else None,
            platform     = _parse_platform(ua),
            open_trades  = digest["open_trades"],
            total_trades = digest["total_trades"],
            unrealized_pl= digest["unrealized_pl"],
            net_pl       = digest["net_pl"],
            win_count    = digest["win_count"],
            loss_count   = digest["loss_count"],
        )
        db.session.add(event)
    except Exception as exc:
        # Never block a login because of the digest — log and continue
        print(f"[login_digest] warning: could not save digest: {exc}")

    db.session.commit()

    token            = generate_token(user.id)
    required_version = Setting.get("app_required_version", "0.0.0")
    download_url     = Setting.get("app_download_url", "")
    return jsonify({
        "token":            token,
        "user":             user.to_dict(),
        "required_version": required_version,
        "download_url":     download_url,
    }), 200


@auth_bp.route("/me", methods=["GET"])
@token_required
def me(current_user):
    return jsonify({"user": current_user.to_dict()}), 200


@auth_bp.route("/login-events", methods=["GET"])
@token_required
def login_events(current_user):
    """
    Return the most recent login events for the authenticated user.
    Optional query param: ?limit=N  (default 20, max 100)
    """
    limit = min(int(request.args.get("limit", 20)), 100)
    events = (
        LoginEvent.query
        .filter_by(user_id=current_user.id)
        .order_by(LoginEvent.logged_in_at.desc())
        .limit(limit)
        .all()
    )
    return jsonify({"events": [e.to_dict() for e in events]}), 200


@auth_bp.route("/forgot-password", methods=["POST"])
def forgot_password():
    """
    Public endpoint — find user by email or username, send a 6-digit reset
    code to their registered email address.  Always returns 200 to avoid
    leaking whether an account exists.
    """
    data       = request.get_json(silent=True) or {}
    identifier = (data.get("identifier") or "").strip().lower()

    if not identifier:
        return jsonify({"error": "Email or username is required"}), 400

    user = User.query.filter(
        (User.email == identifier) | (User.username == identifier)
    ).first()

    # Always respond the same way — don't reveal whether account exists
    if not user:
        return jsonify({"message": "If that account exists, a reset code has been sent."}), 200

    code       = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    from extensions import bcrypt as _bcrypt
    # Store a placeholder hash; will be overwritten at reset time
    pending = PasswordChangeToken.query.filter_by(user_id=user.id).first()
    if pending:
        pending.new_password_hash = ""   # will be set when user submits new password
        pending.code              = code
        pending.expires_at        = expires_at
        pending.created_at        = datetime.now(timezone.utc)
    else:
        pending = PasswordChangeToken(
            user_id           = user.id,
            new_password_hash = "",
            code              = code,
            expires_at        = expires_at,
        )
        db.session.add(pending)
    db.session.commit()

    try:
        _send_verification_email(user.email, code, subject="TradeFinder — your password reset code")
    except Exception as exc:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": f"Failed to send reset email: {exc}"}), 502

    return jsonify({"message": "If that account exists, a reset code has been sent."}), 200


@auth_bp.route("/verify-reset-code", methods=["POST"])
def verify_reset_code():
    """
    Public endpoint — verify the 6-digit code without changing the password yet.
    Returns a short-lived confirmation so the client can proceed to the new-password step.
    """
    data       = request.get_json(silent=True) or {}
    identifier = (data.get("identifier") or "").strip().lower()
    code       = (data.get("code") or "").strip()

    if not identifier or not code:
        return jsonify({"error": "identifier and code are required"}), 400

    user = User.query.filter(
        (User.email == identifier) | (User.username == identifier)
    ).first()

    if not user:
        return jsonify({"error": "Invalid code."}), 400

    pending = PasswordChangeToken.query.filter_by(user_id=user.id).first()
    if not pending:
        return jsonify({"error": "No reset request found. Please request a new code."}), 404

    now = datetime.now(timezone.utc)
    if pending.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": "Code has expired. Please request a new one."}), 410

    if not secrets.compare_digest(pending.code, code):
        return jsonify({"error": "Incorrect code."}), 400

    return jsonify({"ok": True, "user_id": user.id}), 200


@auth_bp.route("/reset-password", methods=["POST"])
def reset_password():
    """
    Public endpoint — verify code + apply new password in one step (final phase).
    """
    data         = request.get_json(silent=True) or {}
    identifier   = (data.get("identifier") or "").strip().lower()
    code         = (data.get("code") or "").strip()
    new_password = data.get("new_password") or ""

    if not identifier or not code or not new_password:
        return jsonify({"error": "identifier, code, and new_password are required"}), 400

    if len(new_password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400

    user = User.query.filter(
        (User.email == identifier) | (User.username == identifier)
    ).first()

    if not user:
        return jsonify({"error": "Invalid reset request."}), 400

    pending = PasswordChangeToken.query.filter_by(user_id=user.id).first()
    if not pending:
        return jsonify({"error": "No reset request found. Please start over."}), 404

    now = datetime.now(timezone.utc)
    if pending.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": "Code has expired. Please start over."}), 410

    if not secrets.compare_digest(pending.code, code):
        return jsonify({"error": "Incorrect code."}), 400

    user.set_password(new_password)
    db.session.delete(pending)
    db.session.commit()
    return jsonify({"message": "Password reset successfully. You can now sign in."}), 200


@auth_bp.route("/profile", methods=["GET"])
@token_required
def get_profile(current_user):
    return jsonify({"user": current_user.to_dict()}), 200


@auth_bp.route("/profile", methods=["PUT"])
@token_required
def update_profile(current_user):
    data = request.get_json(silent=True) or {}

    ALLOWED = ("first_name", "last_name", "address", "city", "state", "zipcode", "mobile_phone")
    for field in ALLOWED:
        if field in data:
            value = (data[field] or "").strip() or None
            setattr(current_user, field, value)

    db.session.commit()
    return jsonify({"user": current_user.to_dict()}), 200


@auth_bp.route("/check-username", methods=["GET"])
@token_required
def check_username(current_user):
    """Return whether a username is available. Authenticated so bots can't enumerate."""
    username = (request.args.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username required"}), 400
    if username == current_user.username:
        return jsonify({"available": True, "same": True}), 200
    taken = User.query.filter(User.username == username).first() is not None
    return jsonify({"available": not taken}), 200


@auth_bp.route("/change-username", methods=["POST"])
@token_required
def change_username(current_user):
    data = request.get_json(silent=True) or {}
    new_username = (data.get("username") or "").strip()

    if not new_username:
        return jsonify({"error": "Username is required"}), 400
    if len(new_username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(new_username) > 64:
        return jsonify({"error": "Username cannot exceed 64 characters"}), 400
    if new_username == current_user.username:
        return jsonify({"error": "That is already your username"}), 400
    if User.query.filter(User.username == new_username).first():
        return jsonify({"error": "Username is already taken"}), 409

    current_user.username = new_username
    db.session.commit()
    return jsonify({"message": "Username updated", "user": current_user.to_dict()}), 200


@auth_bp.route("/request-email-change", methods=["POST"])
@token_required
def request_email_change(current_user):
    """Send a 6-digit verification code to the requested new email address."""
    data      = request.get_json(silent=True) or {}
    new_email = (data.get("email") or "").strip().lower()

    if not new_email:
        return jsonify({"error": "Email is required"}), 400
    if not _EMAIL_RE.match(new_email):
        return jsonify({"error": "Invalid email address"}), 400
    if new_email == current_user.email.lower():
        return jsonify({"error": "That is already your email address"}), 400
    if User.query.filter(User.email == new_email).first():
        return jsonify({"error": "Email address is already in use"}), 409

    code       = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    # Upsert: one pending request per user at a time
    pending = EmailVerification.query.filter_by(user_id=current_user.id).first()
    if pending:
        pending.new_email  = new_email
        pending.code       = code
        pending.expires_at = expires_at
        pending.created_at = datetime.now(timezone.utc)
    else:
        pending = EmailVerification(
            user_id    = current_user.id,
            new_email  = new_email,
            code       = code,
            expires_at = expires_at,
        )
        db.session.add(pending)
    db.session.commit()

    try:
        _send_verification_email(new_email, code, subject="TradeFinder — confirm your new email address")
    except Exception as exc:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": f"Failed to send verification email: {exc}"}), 502

    return jsonify({"message": f"Verification code sent to {new_email}"}), 200


@auth_bp.route("/verify-email-change", methods=["POST"])
@token_required
def verify_email_change(current_user):
    """Confirm the code and update the user's email address."""
    data = request.get_json(silent=True) or {}
    code = (data.get("code") or "").strip()

    if not code:
        return jsonify({"error": "Verification code is required"}), 400

    pending = EmailVerification.query.filter_by(user_id=current_user.id).first()
    if not pending:
        return jsonify({"error": "No pending email change found. Please request a new code."}), 404

    now = datetime.now(timezone.utc)
    if pending.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": "Verification code has expired. Please request a new one."}), 410

    if not secrets.compare_digest(pending.code, code):
        return jsonify({"error": "Incorrect verification code."}), 400

    # Final uniqueness check (race guard)
    if User.query.filter(User.email == pending.new_email, User.id != current_user.id).first():
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": "Email address was taken by another account. Please start over."}), 409

    current_user.email = pending.new_email
    db.session.delete(pending)
    db.session.commit()
    return jsonify({"message": "Email updated successfully.", "user": current_user.to_dict()}), 200


@auth_bp.route("/request-password-change", methods=["POST"])
@token_required
def request_password_change(current_user):
    """
    Validate the new password, send a 6-digit code to the user's email.
    The new password hash is stored until the code is verified.
    """
    data         = request.get_json(silent=True) or {}
    new_password = data.get("new_password") or ""

    if len(new_password) < 8:
        return jsonify({"error": "New password must be at least 8 characters"}), 400

    from extensions import bcrypt
    new_hash   = bcrypt.generate_password_hash(new_password).decode("utf-8")
    code       = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    # Upsert — one pending request per user
    pending = PasswordChangeToken.query.filter_by(user_id=current_user.id).first()
    if pending:
        pending.new_password_hash = new_hash
        pending.code              = code
        pending.expires_at        = expires_at
        pending.created_at        = datetime.now(timezone.utc)
    else:
        pending = PasswordChangeToken(
            user_id           = current_user.id,
            new_password_hash = new_hash,
            code              = code,
            expires_at        = expires_at,
        )
        db.session.add(pending)
    db.session.commit()

    # Send code to the user's current email
    try:
        _send_verification_email(current_user.email, code, subject="TradeFinder — confirm your password change")
    except Exception as exc:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": f"Failed to send verification email: {exc}"}), 502

    masked = current_user.email[:2] + "***@" + current_user.email.split("@")[-1]
    return jsonify({"message": f"Verification code sent to {masked}"}), 200


@auth_bp.route("/verify-password-change", methods=["POST"])
@token_required
def verify_password_change(current_user):
    """Apply the pre-hashed new password once the correct code is provided."""
    data = request.get_json(silent=True) or {}
    code = (data.get("code") or "").strip()

    if not code:
        return jsonify({"error": "Verification code is required"}), 400

    pending = PasswordChangeToken.query.filter_by(user_id=current_user.id).first()
    if not pending:
        return jsonify({"error": "No pending password change found. Please start over."}), 404

    now = datetime.now(timezone.utc)
    if pending.expires_at.replace(tzinfo=timezone.utc) < now:
        db.session.delete(pending)
        db.session.commit()
        return jsonify({"error": "Verification code has expired. Please start over."}), 410

    if not secrets.compare_digest(pending.code, code):
        return jsonify({"error": "Incorrect verification code."}), 400

    current_user.password_hash = pending.new_password_hash
    db.session.delete(pending)
    db.session.commit()
    return jsonify({"message": "Password updated successfully."}), 200
