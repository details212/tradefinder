"""
Leaderboard — system performance criteria (settings) and per-user trading stats.

GET /api/leaderboard/
  - performance_settings: rows from `settings` whose key starts with `performance_`
  - entries: one row per user with full closed-trade metrics
  - rank_by: from setting `performance_rank_by` (default net_pl)

Supported rank_by values:
  net_pl | win_rate | profit_factor | expectancy | closed_trades | wins | avg_rr
"""

import math
from flask import Blueprint, jsonify
from sqlalchemy import and_
from auth import token_required
from extensions import db
from models import Order, Setting, User

leaderboard_bp = Blueprint("leaderboard", __name__, url_prefix="/api/leaderboard")

_RANK_KEYS = frozenset(
    {"net_pl", "win_rate", "profit_factor", "expectancy", "closed_trades", "wins", "avg_rr"}
)

_RANK_LABELS = {
    "net_pl": "Net P&L",
    "win_rate": "Win rate",
    "profit_factor": "Profit factor",
    "expectancy": "Expectancy",
    "closed_trades": "Closed trades",
    "wins": "Wins",
    "avg_rr": "Avg R/R",
}


def _metrics(closed_orders, open_count: int, user_id: int, current_user_id: int) -> dict:
    """Compute all leaderboard metrics from a list of closed Order objects."""
    pls = [float(o.unrealized_pl) for o in closed_orders]

    c = len(pls)
    if c == 0:
        return {
            "closed_trades": 0,
            "open_trades": open_count,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "net_pl": 0.0,
            "avg_trade": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_factor": None,
            "expectancy": None,
            "best_trade": None,
            "worst_trade": None,
            "avg_rr": None,
        }

    winners = [p for p in pls if p > 0]
    losers  = [p for p in pls if p < 0]

    total_win  = sum(winners)
    total_loss = sum(losers)

    win_n  = len(winners)
    loss_n = len(losers)
    win_rate = win_n / c

    avg_win  = total_win  / win_n  if win_n  else None
    avg_loss = total_loss / loss_n if loss_n else None

    profit_factor = (
        (total_win / abs(total_loss)) if total_loss < 0 else None
    )

    expectancy = None
    if avg_win is not None or avg_loss is not None:
        aw = avg_win  or 0.0
        al = avg_loss or 0.0
        expectancy = win_rate * aw + (1 - win_rate) * al

    # Avg effective R/R from trades that have it
    rr_vals = [
        float(o.rr_ratio_effective)
        for o in closed_orders
        if o.rr_ratio_effective is not None
    ]
    avg_rr = (sum(rr_vals) / len(rr_vals)) if rr_vals else None

    return {
        "closed_trades": c,
        "open_trades": open_count,
        "wins": win_n,
        "losses": loss_n,
        "win_rate": round(win_rate, 4),
        "net_pl": round(sum(pls), 2),
        "avg_trade": round(sum(pls) / c, 2),
        "avg_win": round(avg_win, 2) if avg_win is not None else None,
        "avg_loss": round(avg_loss, 2) if avg_loss is not None else None,
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "expectancy": round(expectancy, 2) if expectancy is not None else None,
        "best_trade": round(max(pls), 2),
        "worst_trade": round(min(pls), 2),
        "avg_rr": round(avg_rr, 3) if avg_rr is not None else None,
    }


@leaderboard_bp.route("/", methods=["GET"])
@token_required
def get_leaderboard(current_user):
    rank_by = (Setting.get("performance_rank_by") or "net_pl").strip().lower()
    if rank_by not in _RANK_KEYS:
        rank_by = "net_pl"

    perf_rows = (
        Setting.query.filter(Setting.key.startswith("performance_"))
        .order_by(Setting.key)
        .all()
    )
    performance_settings = [
        {
            "key": r.key,
            "value": r.value,
            "description": r.description,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in perf_rows
    ]

    # Fetch all orders in one query; group in Python for flexibility
    all_orders = (
        db.session.query(Order)
        .join(User, User.id == Order.user_id)
        .filter(User.is_active.is_(True))
        .all()
    )

    users = User.query.filter_by(is_active=True).order_by(User.username).all()

    # Group orders by user
    by_user: dict[int, list] = {u.id: [] for u in users}
    open_by_user: dict[int, int] = {u.id: 0 for u in users}

    for o in all_orders:
        if o.user_id not in by_user:
            continue
        is_closed = o.is_open is False and o.unrealized_pl is not None
        if is_closed:
            by_user[o.user_id].append(o)
        elif o.is_open is True or o.is_open is None:
            open_by_user[o.user_id] += 1

    user_map = {u.id: u for u in users}
    entries = []
    for uid, closed_orders in by_user.items():
        u = user_map[uid]
        m = _metrics(closed_orders, open_by_user[uid], uid, current_user.id)
        entries.append(
            {
                "user_id": uid,
                "username": u.username,
                "is_you": uid == current_user.id,
                **m,
            }
        )

    # Sort — None values sort last
    def sort_key(e):
        v = e.get(rank_by)
        return (v is None, -(v or 0) if rank_by != "avg_rr" else -(v or 0))

    entries.sort(key=sort_key)

    for i, e in enumerate(entries, start=1):
        e["rank"] = i

    return jsonify(
        {
            "rank_by": rank_by,
            "rank_labels": _RANK_LABELS,
            "performance_settings": performance_settings,
            "entries": entries,
        }
    ), 200
