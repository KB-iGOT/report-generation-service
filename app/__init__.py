from flask import Flask, g
from flask_sqlalchemy import SQLAlchemy
import logging
import os
import gc
from app.authentication.KeyManager import KeyManager
from constants import ACCESS_TOKEN_PUBLICKEY_BASEPATH, IS_VALIDATION_ENABLED

db = SQLAlchemy()

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    # app.config.from_object('app.config.db_config.Config')
    
    # try:
    #     # Initialize the database connection pool with a reasonable size
    #     DBConnection.initialize_pool(1,10)  # Adjust based on load
    #     db.init_app(app)
    #     with app.app_context():
    #         db.session.execute('SELECT 1')
    #         logger.info("Database connection tested successfully.")
    # except Exception as e:
    #     logger.error(f"Database initialization failed: {e}")
    #     raise RuntimeError("Application startup aborted due to database connection failure.")

    if IS_VALIDATION_ENABLED.lower() == 'true':
        base_path = ACCESS_TOKEN_PUBLICKEY_BASEPATH
        if not base_path:
            logger.error("ACCESS_TOKEN_PUBLICKEY_BASEPATH is not set.")
            raise ValueError("ACCESS_TOKEN_PUBLICKEY_BASEPATH is not set.")
        KeyManager.init(base_path)
        logger.info("KeyManager initialized successfully.")

    try:
        from app.controllers.report_controller import report_controller
        app.register_blueprint(report_controller)
        from app.controllers.report_controller_v2 import report_controller_v2
        app.register_blueprint(report_controller_v2)
        from app.controllers.health_controller import health_controller
        app.register_blueprint(health_controller)
    except Exception as e:
        app.logger.error(f"Blueprint registration failed: {e}")
        raise

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        #db.session.remove()
        # Force garbage collection after request
        gc.collect()

    return app