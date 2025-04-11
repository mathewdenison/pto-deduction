import os
import json
import logging
import base64

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import the shared code from your PyPI package.
from pto_common_timesheet_mfdenison_hopkinsep.utils.pto_update_manager import PTOUpdateManager
from pto_common_timesheet_mfdenison_hopkinsep.utils.dashboard_events import build_dashboard_payload

# Set up Google Cloud Logging
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Configure standard logging
logger = logging.getLogger("pto_deduction_worker")
logger.setLevel(logging.INFO)

# Pub/Sub publisher configuration
PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
publisher = pubsub_v1.PublisherClient()

def pto_deduction_pubsub(event, context):
    """
    Cloud Function triggered by a Pub/Sub message for PTO deduction.

    The message is expected to be a JSON payload (possibly double-encoded) that contains an 'employee_id'
    and either a 'pto_deduction' field or a nested 'data.pto_hours' field.

    The function processes the deduction and publishes an update payload to a dashboard Pub/Sub topic.
    """
    try:
        # Decode the Pub/Sub message data, which is base64 encoded.
        raw_data = base64.b64decode(event['data']).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # First decode: try to load as JSON.
        update_data = json.loads(raw_data)
        # If update_data itself is still a string, parse it again.
        if isinstance(update_data, str):
            update_data = json.loads(update_data)
        logger.info(f"Message payload: {update_data}")

        employee_id = update_data["employee_id"]

        # Determine pto_deduction; if not present, try pto_hours from nested data.
        pto_deduction = update_data.get("pto_deduction")
        if pto_deduction is None:
            pto_value = update_data.get("data", {}).get("pto_hours")
            if pto_value is not None:
                try:
                    pto_deduction = int(pto_value)
                    logger.info("Converted pto_hours '%s' to integer: %d", pto_value, pto_deduction)
                except Exception as conv_error:
                    logger.error("Failed to convert pto_hours '%s' to int: %s", pto_value, conv_error)
                    pto_deduction = 0
            else:
                pto_deduction = 0

        # Process the deduction by creating an instance of PTOUpdateManager.
        updater = PTOUpdateManager(employee_id)
        result = updater.subtract_pto(pto_deduction)

        # Retrieve the updated balance if available.
        new_balance = updater.get_current_balance() if hasattr(updater, "get_current_balance") else "unknown"

        # Build an appropriate dashboard payload depending on success or failure.
        if result["result"] == "success":
            log_msg = f"[SUCCESS] PTO for employee {employee_id} updated. New balance: {new_balance}"
            logger.info(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "refresh_data",
                "Please refresh dashboard data.",
                {}
            )
        else:
            log_msg = f"[ERROR] Failed to update PTO for employee {employee_id}. Reason: {result['message']}"
            logger.error(log_msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "pto_updated",
                log_msg
            )

        # Publish the update payload to the dashboard Pub/Sub topic.
        publisher.publish(DASHBOARD_TOPIC, json.dumps(dashboard_payload).encode("utf-8"))
        logger.info("Published update to dashboard Pub/Sub topic.")

    except Exception as e:
        logger.exception(f"Error processing message: {str(e)}")
        # Raising exception signals the function to fail, which may trigger a retry.
        raise
