from datetime import datetime, timezone
from extensions import db, bcrypt


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime, nullable=True)

    # Contact / profile fields
    first_name   = db.Column(db.String(80),  nullable=True)
    last_name    = db.Column(db.String(80),  nullable=True)
    address      = db.Column(db.String(255), nullable=True)
    city         = db.Column(db.String(100), nullable=True)
    state        = db.Column(db.String(2),   nullable=True)
    zipcode      = db.Column(db.String(10),  nullable=True)
    mobile_phone = db.Column(db.String(15),  nullable=True)

    watchlist = db.relationship("WatchlistItem", backref="user", lazy=True, cascade="all, delete-orphan")

    def set_password(self, password: str) -> None:
        self.password_hash = bcrypt.generate_password_hash(password).decode("utf-8")

    def check_password(self, password: str) -> bool:
        return bcrypt.check_password_hash(self.password_hash, password)

    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "username":     self.username,
            "email":        self.email,
            "is_active":    self.is_active,
            "created_at":   self.created_at.isoformat(),
            "first_name":   self.first_name,
            "last_name":    self.last_name,
            "address":      self.address,
            "city":         self.city,
            "state":        self.state,
            "zipcode":      self.zipcode,
            "mobile_phone": self.mobile_phone,
        }


class Setting(db.Model):
    __tablename__ = "settings"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    value = db.Column(db.Text, nullable=True)
    description = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def get(cls, key: str, default=None):
        row = cls.query.filter_by(key=key).first()
        return row.value if row else default

    @classmethod
    def set(cls, key: str, value: str, description: str = None):
        row = cls.query.filter_by(key=key).first()
        if row:
            row.value = value
            if description:
                row.description = description
        else:
            row = cls(key=key, value=value, description=description)
            db.session.add(row)
        db.session.commit()
        return row

    def to_dict(self):
        return {
            "key": self.key,
            "value": self.value,
            "description": self.description,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class BrokerCredential(db.Model):
    """Per-user broker credentials stored as key/value pairs."""
    __tablename__ = "broker_credentials"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    broker     = db.Column(db.String(50),  nullable=False)   # e.g. "alpaca"
    key        = db.Column(db.String(100), nullable=False)   # e.g. "api_key"
    value      = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint("user_id", "broker", "key", name="uq_user_broker_key"),
    )

    @classmethod
    def get(cls, user_id: int, broker: str, key: str, default=None):
        row = cls.query.filter_by(user_id=user_id, broker=broker, key=key).first()
        return row.value if row else default

    @classmethod
    def set(cls, user_id: int, broker: str, key: str, value: str):
        row = cls.query.filter_by(user_id=user_id, broker=broker, key=key).first()
        if row:
            row.value = value
        else:
            row = cls(user_id=user_id, broker=broker, key=key, value=value)
            db.session.add(row)
        db.session.commit()
        return row

    @classmethod
    def get_all(cls, user_id: int, broker: str) -> dict:
        rows = cls.query.filter_by(user_id=user_id, broker=broker).all()
        return {r.key: r.value for r in rows}


class MaCache(db.Model):
    """
    Caches the five daily moving-average values (EMA10, EMA20, SMA50, SMA150,
    SMA200) for a ticker.  Records older than 24 h are treated as stale and
    refreshed on demand.
    """
    __tablename__ = "ma_cache"

    ticker     = db.Column(db.String(10), primary_key=True, nullable=False)
    ema10      = db.Column(db.Numeric(18, 4), nullable=True)
    ema20      = db.Column(db.Numeric(18, 4), nullable=True)
    sma50      = db.Column(db.Numeric(18, 4), nullable=True)
    sma150     = db.Column(db.Numeric(18, 4), nullable=True)
    sma200     = db.Column(db.Numeric(18, 4), nullable=True)
    fetched_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    def is_fresh(self, ttl_hours: int = 24) -> bool:
        if not self.fetched_at:
            return False
        age = datetime.now(timezone.utc) - self.fetched_at.replace(tzinfo=timezone.utc)
        return age.total_seconds() < ttl_hours * 3600

    def to_dict(self) -> dict:
        return {
            "ticker":  self.ticker,
            "ema10":   float(self.ema10)  if self.ema10  is not None else None,
            "ema20":   float(self.ema20)  if self.ema20  is not None else None,
            "sma50":   float(self.sma50)  if self.sma50  is not None else None,
            "sma150":  float(self.sma150) if self.sma150 is not None else None,
            "sma200":  float(self.sma200) if self.sma200 is not None else None,
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
        }


class Order(db.Model):
    """
    Persists every bracket order placed through the app so a user can later
    reconstruct the chart drawing from the saved levels.

    Chart-reconstruction fields
    ---------------------------
    ticker        — the stock symbol
    bias          — directional bias from the Trade Idea ("long" / "short")
    direction     — actual trade direction ("long" / "short")
    bar_time      — ISO8601 string of the signal bar (ET) from ModalChart props
    threshold     — resistance/support level shown on the chart (ModalChart props)

    Order fields
    ------------
    order_type    — "limit" | "market"
    qty           — number of shares
    entry_price   — limit price sent to broker
    stop_price    — stop-loss price
    target_price  — take-profit limit price
    rr_ratio      — reward/risk ratio at time of placement
    risk_amt      — dollar risk  (|entry − stop| × qty)
    reward_amt    — dollar reward (|target − entry| × qty)

    Broker response
    ---------------
    alpaca_order_id  — parent order ID returned by Alpaca
    alpaca_client_id — client_order_id echo (for reconciliation)
    status           — initial status string from Alpaca (e.g. "accepted")
    paper_mode       — True if placed on paper account
    raw_response     — full JSON response stored as Text for future use
    """
    __tablename__ = "orders"

    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)

    # Chart reconstruction
    ticker           = db.Column(db.String(10),  nullable=False)
    bias             = db.Column(db.String(20),  nullable=True)
    direction        = db.Column(db.String(10),  nullable=False)  # "long" | "short"
    bar_time         = db.Column(db.String(50),  nullable=True)   # signal bar (ET string from Trade Ideas)
    threshold        = db.Column(db.Numeric(18, 4), nullable=True)
    entry_time       = db.Column(db.BigInteger,  nullable=True)   # UTC ms timestamp of the bar clicked for entry (rr.entryTime)

    # Order parameters
    order_type       = db.Column(db.String(20),  nullable=False)  # "limit" | "market"
    qty              = db.Column(db.Integer,     nullable=False)
    entry_price      = db.Column(db.Numeric(18, 4), nullable=True)
    stop_price       = db.Column(db.Numeric(18, 4), nullable=False)
    target_price     = db.Column(db.Numeric(18, 4), nullable=False)
    rr_ratio          = db.Column(db.Numeric(10, 4), nullable=True)   # user's intended R/R input
    rr_ratio_effective = db.Column(db.Numeric(10, 4), nullable=True)  # actual ratio computed from entry/stop/target prices
    risk_amt          = db.Column(db.Numeric(18, 4), nullable=True)
    reward_amt        = db.Column(db.Numeric(18, 4), nullable=True)

    # Broker response
    alpaca_order_id  = db.Column(db.String(100), nullable=True, index=True)
    alpaca_client_id = db.Column(db.String(100), nullable=True)
    status           = db.Column(db.String(50),  nullable=True)
    paper_mode       = db.Column(db.Boolean,     nullable=False, default=True)
    raw_response     = db.Column(db.Text,        nullable=True)

    # Live P/L tracking (populated/refreshed on every sync)
    filled_avg_price = db.Column(db.Numeric(18, 4), nullable=True)  # actual fill price from Alpaca
    current_price    = db.Column(db.Numeric(18, 4), nullable=True)  # price at last sync
    unrealized_pl    = db.Column(db.Numeric(18, 4), nullable=True)  # (current − fill) × qty × side
    synced_at        = db.Column(db.DateTime,       nullable=True)  # when P/L was last refreshed
    is_open          = db.Column(db.Boolean,        nullable=True,  default=True)  # Alpaca-authoritative open flag

    created_at       = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "user_id":          self.user_id,
            "ticker":           self.ticker,
            "bias":             self.bias,
            "direction":        self.direction,
            "bar_time":         self.bar_time,
            "threshold":        float(self.threshold)    if self.threshold    is not None else None,
            "entry_time":       self.entry_time,
            "order_type":       self.order_type,
            "qty":              self.qty,
            "entry_price":      float(self.entry_price)  if self.entry_price  is not None else None,
            "stop_price":       float(self.stop_price),
            "target_price":     float(self.target_price),
            "rr_ratio":           float(self.rr_ratio)           if self.rr_ratio           is not None else None,
            "rr_ratio_effective": float(self.rr_ratio_effective) if self.rr_ratio_effective is not None else None,
            "risk_amt":         float(self.risk_amt)     if self.risk_amt     is not None else None,
            "reward_amt":       float(self.reward_amt)   if self.reward_amt   is not None else None,
            "alpaca_order_id":  self.alpaca_order_id,
            "alpaca_client_id": self.alpaca_client_id,
            "status":           self.status,
            "paper_mode":       self.paper_mode,
            "filled_avg_price": float(self.filled_avg_price) if self.filled_avg_price is not None else None,
            "current_price":    float(self.current_price)    if self.current_price    is not None else None,
            "unrealized_pl":    float(self.unrealized_pl)    if self.unrealized_pl    is not None else None,
            "synced_at":        self.synced_at.isoformat()   if self.synced_at        is not None else None,
            "is_open":          self.is_open,
            "created_at":       self.created_at.isoformat()  if self.created_at       is not None else None,
        }


class LoginEvent(db.Model):
    """
    One row per successful login.  Captures connection details and a snapshot
    of the user's trading account at the moment they authenticated.
    """
    __tablename__ = "login_events"

    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    logged_in_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    # Connection / client details
    ip_address   = db.Column(db.String(45),  nullable=True)   # IPv4 or IPv6
    user_agent   = db.Column(db.String(512), nullable=True)
    platform     = db.Column(db.String(120), nullable=True)   # parsed OS / app name

    # Account snapshot — computed from the DB at login time
    open_trades  = db.Column(db.Integer,         nullable=True)  # orders not in closed status
    total_trades = db.Column(db.Integer,         nullable=True)  # all-time order count
    unrealized_pl= db.Column(db.Numeric(18, 4),  nullable=True)  # sum of open order P/L
    net_pl       = db.Column(db.Numeric(18, 4),  nullable=True)  # sum of all closed order P/L
    win_count    = db.Column(db.Integer,         nullable=True)  # closed trades with pl > 0
    loss_count   = db.Column(db.Integer,         nullable=True)  # closed trades with pl < 0

    def to_dict(self) -> dict:
        return {
            "id":            self.id,
            "user_id":       self.user_id,
            "logged_in_at":  self.logged_in_at.isoformat() if self.logged_in_at else None,
            "ip_address":    self.ip_address,
            "user_agent":    self.user_agent,
            "platform":      self.platform,
            "open_trades":   self.open_trades,
            "total_trades":  self.total_trades,
            "unrealized_pl": float(self.unrealized_pl) if self.unrealized_pl is not None else None,
            "net_pl":        float(self.net_pl)        if self.net_pl        is not None else None,
            "win_count":     self.win_count,
            "loss_count":    self.loss_count,
        }


class RegistrationToken(db.Model):
    """
    Holds a pending email-verification for new account registration.
    One row per email address.  Verified flag is set after the code is confirmed.
    Expires after 30 minutes.
    """
    __tablename__ = "registration_tokens"

    id         = db.Column(db.Integer, primary_key=True)
    email      = db.Column(db.String(120), nullable=False, unique=True, index=True)
    code       = db.Column(db.String(6),   nullable=False)
    verified   = db.Column(db.Boolean,     nullable=False, default=False)
    expires_at = db.Column(db.DateTime,    nullable=False)
    created_at = db.Column(db.DateTime,    default=datetime.utcnow)


class PasswordChangeToken(db.Model):
    """
    Holds a pending password-change request.  One row per user at most.
    Expires after 15 minutes.
    """
    __tablename__ = "password_change_tokens"

    id            = db.Column(db.Integer, primary_key=True)
    user_id       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    new_password_hash = db.Column(db.String(256), nullable=False)   # pre-hashed; applied on verify
    code          = db.Column(db.String(6),   nullable=False)
    expires_at    = db.Column(db.DateTime,    nullable=False)
    created_at    = db.Column(db.DateTime,    default=datetime.utcnow)


class EmailVerification(db.Model):
    """
    Holds a pending email-change request.  One row per user at most — a new
    request overwrites the previous one.  Expires after 15 minutes.
    """
    __tablename__ = "email_verifications"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    new_email  = db.Column(db.String(120), nullable=False)
    code       = db.Column(db.String(6),   nullable=False)
    expires_at = db.Column(db.DateTime,    nullable=False)
    created_at = db.Column(db.DateTime,    default=datetime.utcnow)


class ChartBox(db.Model):
    __tablename__ = "chart_boxes"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    ticker     = db.Column(db.String(20), nullable=False)
    x1         = db.Column(db.BigInteger)           # left edge  – ms timestamp
    x2         = db.Column(db.BigInteger)           # right edge – ms timestamp
    y1         = db.Column(db.Numeric(18, 4))       # top price
    y2         = db.Column(db.Numeric(18, 4))       # bottom price
    color      = db.Column(db.String(20), default="#ffd700")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.Index("idx_chart_boxes_user_ticker", "user_id", "ticker"),
    )


class WatchlistItem(db.Model):
    __tablename__ = "watchlist"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    ticker = db.Column(db.String(10), nullable=False)
    bias = db.Column(db.String(20), nullable=True)
    threshold = db.Column(db.Numeric(18, 4), nullable=True)
    bar_time = db.Column(db.DateTime, nullable=True)
    # Where the favorite was added: tradeideas | patternanalysis | stocks (main stock view)
    source = db.Column(db.String(32), nullable=True)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint("user_id", "ticker", name="unique_user_ticker"),)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ticker": self.ticker,
            "bias": self.bias,
            "threshold": float(self.threshold) if self.threshold is not None else None,
            "bar_time": self.bar_time.isoformat() if self.bar_time is not None else None,
            "source": self.source,
            "added_at": self.added_at.isoformat(),
        }
