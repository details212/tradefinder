"""
Delete a user and all associated per-user data in the database.

Where data lives (see models.py and routes):
  - Trades / bracket orders: table `orders` (Order), column user_id — written from
    routes/alpaca_routes.py when orders are placed and synced.
  - Per-user settings & secrets: table `broker_credentials` — Alpaca API keys
    (broker='alpaca') and UI preferences (broker='preferences', see preferences_routes.py).
  - Chart drawings: `chart_boxes` (scanner_routes).
  - Favorites: `watchlist` (stock_routes).
  - Auth / audit: `login_events`, `password_change_tokens`, `email_verifications`.
  - Optional: `registration_tokens` row matching the user's email (pre-signup flow).

The global `settings` table (Setting model) is application-wide (e.g. Polygon key) and is
NOT removed by this script.

Usage:
  python deleteuser.py --userid 1
  python deleteuser.py --userid 1 --yes          # skip confirmation prompt
  python deleteuser.py --userid 1 --dry-run     # show counts only
"""

from __future__ import annotations

import argparse
import sys

from app import create_app
from extensions import db
from models import (
    User,
    Order,
    BrokerCredential,
    LoginEvent,
    PasswordChangeToken,
    EmailVerification,
    ChartBox,
    WatchlistItem,
    RegistrationToken,
)


def counts_for_user(user_id: int) -> dict[str, int]:
    return {
        "orders": Order.query.filter_by(user_id=user_id).count(),
        "broker_credentials": BrokerCredential.query.filter_by(user_id=user_id).count(),
        "login_events": LoginEvent.query.filter_by(user_id=user_id).count(),
        "password_change_tokens": PasswordChangeToken.query.filter_by(user_id=user_id).count(),
        "email_verifications": EmailVerification.query.filter_by(user_id=user_id).count(),
        "chart_boxes": ChartBox.query.filter_by(user_id=user_id).count(),
        "watchlist": WatchlistItem.query.filter_by(user_id=user_id).count(),
    }


def registration_token_count(email: str | None) -> int:
    if not email:
        return 0
    return RegistrationToken.query.filter_by(email=email).count()


def delete_user_data(user_id: int, email: str | None) -> None:
    Order.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    BrokerCredential.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    LoginEvent.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    PasswordChangeToken.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    EmailVerification.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    ChartBox.query.filter_by(user_id=user_id).delete(synchronize_session=False)
    WatchlistItem.query.filter_by(user_id=user_id).delete(synchronize_session=False)

    if email:
        RegistrationToken.query.filter_by(email=email).delete(synchronize_session=False)

    User.query.filter_by(id=user_id).delete(synchronize_session=False)
    db.session.commit()


def main() -> int:
    p = argparse.ArgumentParser(description="Delete a user and all related DB rows.")
    p.add_argument("--userid", type=int, required=True, help="users.id to delete")
    p.add_argument("--yes", action="store_true", help="Do not ask for confirmation")
    p.add_argument("--dry-run", action="store_true", help="Print counts only; do not delete")
    args = p.parse_args()

    app = create_app()
    with app.app_context():
        user = User.query.filter_by(id=args.userid).first()
        if not user:
            print(f"No user with id={args.userid}.", file=sys.stderr)
            return 1

        c = counts_for_user(args.userid)
        rt = registration_token_count(user.email)
        print(f"User id={user.id} username={user.username!r} email={user.email!r}")
        print("Rows to remove:")
        for k, v in c.items():
            print(f"  {k}: {v}")
        print(f"  registration_tokens (matching email): {rt}")

        if args.dry_run:
            print("--dry-run: no changes made.")
            return 0

        if not args.yes:
            s = input("Type DELETE to confirm permanent removal: ").strip()
            if s != "DELETE":
                print("Aborted.")
                return 2

        delete_user_data(args.userid, user.email)
        print(f"Deleted user {args.userid} and associated data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
