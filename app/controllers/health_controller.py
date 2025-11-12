from flask import Blueprint, jsonify
import logging
from ..services.fetch_data_bigQuery import BigQueryService

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

health_controller = Blueprint('health_controller', __name__)

@health_controller.route('/health', methods=['GET'])
def health_check():
    try:
        bigquery_service = BigQueryService()  # Create an instance of BigQueryService
        bigquery_service.test_connection()  # Call the method on the instance
        return jsonify({"status": "True", "BigQueryDB": {"status": "Connected"}}), 200
        # Check PostgreSQL connection using DBConnection
        # with DBConnection.get_connection() as connection:
        #     with connection.cursor() as cursor:  # Create a cursor
        #         cursor.execute("SELECT 1")  # Use the cursor to execute the query
        #         result = cursor.fetchone()  # Fetch the result
        #         if result and result[0] == 1:
        #             logger.info("PostgreSQL connection is healthy.")
        #             return jsonify({"status": "True", "postgresDB": {"status": "Connected"}}), 200
                #         else:
                #             raise Exception("Invalid response from database")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            "status": "False",
            "BiqQueryDB": {"status": "down"}
        }), 500

@health_controller.route('/liveness', methods=['GET'])
def liveness_check():
    logger.info("Liveness check endpoint called.")
    return jsonify({"status": "OK"}), 200
