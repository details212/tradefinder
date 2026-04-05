from flask import Flask, jsonify
from config import Config
from extensions import db, bcrypt, cors
from db_migrations import apply_runtime_migrations


def create_app(config_class=Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    bcrypt.init_app(app)
    cors.init_app(app, resources={r"/api/*": {"origins": "*"}})

    from routes.auth_routes import auth_bp
    from routes.stock_routes import stock_bp
    from routes.settings_routes import settings_bp
    from routes.tradeideas_routes import tradeideas_bp
    from routes.snapshots_routes import snapshots_bp
    from routes.alpaca_routes import alpaca_bp
    from routes.preferences_routes import preferences_bp
    from routes.resources_routes import resources_bp
    from routes.scanner_routes import scanner_bp
    from routes.leaderboard_routes import leaderboard_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(stock_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(tradeideas_bp)
    app.register_blueprint(snapshots_bp)
    app.register_blueprint(alpaca_bp)
    app.register_blueprint(preferences_bp)
    app.register_blueprint(resources_bp)
    app.register_blueprint(scanner_bp)
    app.register_blueprint(leaderboard_bp)

    @app.route("/api/health")
    def health():
        return jsonify({"status": "ok"}), 200

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def server_error(e):
        return jsonify({"error": "Internal server error"}), 500

    with app.app_context():
        db.create_all()
        apply_runtime_migrations(db)

    # Start background services (only in the main Werkzeug process)
    import os
    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        from snapshot_service import start_snapshot_updater
        start_snapshot_updater(app)

        from ma_cache_service import start_ma_cache_updater, start_priority_ma_refresher
        start_ma_cache_updater(app)
        start_priority_ma_refresher(app)

        from tp_auto_close_service import start_tp_auto_close
        start_tp_auto_close(app)

    return app


if __name__ == "__main__":
    import os
    app = create_app()
    debug = os.getenv("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=5000, debug=debug)
