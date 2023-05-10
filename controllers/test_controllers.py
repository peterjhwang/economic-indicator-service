from flask_app import application
from flask import request, jsonify
from utils.aws import send_message
from services.data_pipeline.stats_api_service import stats_api_refresh


@application.route("/send_email", methods=["POST"])
def send_email():
    """
    endpoint to send an email using AWS SNS service
    """
    try:
        req = request.get_json()
        typ, message = req["type"], req["message"]
        response = send_message(typ, message)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return jsonify({"message": "successfully sent"})
        else:
            return jsonify({"error": response})
    except Exception as e:
        return jsonify({"error": str(e)})


@application.route("/stats_api")
def refresh_stats_api():
    """
    Manually trigger the stats_api_refresh function
    """
    application.logger.info("/stats_api")
    try:
        stats_api_refresh()
        return jsonify({"message": "Stats data successfully refreshed"})
    except Exception as e:
        return jsonify({"Error message": str(e)})


@application.route("/test")
def api_test():
    return jsonify({"message": "API is running"})
