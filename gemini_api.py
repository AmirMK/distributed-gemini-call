import os
import json
import time
import requests
import yaml
import logging

import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting, Part, GenerationConfig

from google.cloud import tasks_v2
from google.cloud import functions_v1
from google.cloud import storage

# --------------------------- Configuration ---------------------------
PROJECT_ID = os.environ.get("PROJECT_ID")  # Or hardcode your project ID
LOCATION = os.environ.get("LOCATION", "us-central1")  # Or hardcode the region
QUEUE_ID = "api-call-queue"  # Name of your Cloud Tasks queue
FUNCTION_NAME = "api-call-worker"

# --------------------------- Logging Setup ---------------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --------------------------- Load Queue Configuration ---------------------------
def _load_queue_config():
    """Loads the queue configuration from the YAML file ONCE."""
    try:
        with open("queue_config.yaml", "r") as f:
            config = yaml.safe_load(f)
        logging.info("Queue configuration loaded from YAML.")
        return config
    except Exception as e:
        logging.exception("Error loading queue_config.yaml:")
        return {}  # Return an empty dictionary in case of error


QUEUE_CONFIG = _load_queue_config()  # Load the config when the module is initialized


def call_gemini(video_url):
    """
    Calls the Gemini model to categorize a video ad.

    Args:
        video_url: The GCS URL of the video file.

    Returns:
        The Gemini response or an error message (including the URL).
    """
    try:
        logging.info(f"URL sent to Gemini: {video_url}")  # Log before sending to Gemini

        response_schema = {
            "type": "object",
            "properties": {
                "IAB_Category": {
                    "type": "STRING"
                },
            },
            "required": ["IAB_Category"]
        }

        vertexai.init(project=PROJECT_ID, location=LOCATION)

        generation_config = GenerationConfig(
            max_output_tokens=8192,
            temperature=0.0,
            response_mime_type="application/json",
            response_schema=response_schema
        )

        video = Part.from_uri(
            mime_type="video/mp4",
            uri=video_url,
        )

        model = GenerativeModel(
            "gemini-1.5-pro-002",
            system_instruction="You categorize a video ad into IAB categories. You return the name of only one IAB category which is the most relevant one"
        )

        responses = model.generate_content(
            [video, "Please classify"],
            generation_config=generation_config,
        )

        return responses

    except Exception as e:
        logging.exception(f"Gemini call failed for URL: {video_url}")
        return f"Gemini call failed for URL: {video_url}. Error: {e}"


def create_cloud_task(url):
    """
    Creates a Cloud Task to execute the API call.

    Args:
        url: The URL to pass to the API call worker function.
    """
    client = tasks_v2.CloudTasksClient()
    queue_path = client.queue_path(PROJECT_ID, LOCATION, QUEUE_CONFIG.get('queue_id'))

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/{FUNCTION_NAME}",  # URL of Cloud Function 2
            "headers": {"Content-type": "application/json"},
            "body": json.dumps({"url": url}).encode(),
        }
    }

    response = client.create_task(request={"parent": queue_path, "task": task})
    logging.info(f"Created task: {response.name}. URL in queue: {url}")  # Log URL in queue
    return response.name


def create_queue():
    """Creates the Cloud Tasks queue if it doesn't exist."""
    client = tasks_v2.CloudTasksClient()
    queue_id = QUEUE_CONFIG.get('queue_id')
    queue_path = client.queue_path(PROJECT_ID, LOCATION, queue_id)

    try:
        client.get_queue(request={"name": queue_path})
        logging.info(f"Queue '{queue_id}' already exists.")
    except Exception as e:
        if "NOT_FOUND" in str(e):
            # Queue doesn't exist, create it
            queue = {
                "name": queue_path,
                "rate_limits": QUEUE_CONFIG.get("rate_limits", {}),
                "retry_config": QUEUE_CONFIG.get("retry_config", {}),
            }
            client.create_queue(
                request={"parent": client.location_path(PROJECT_ID, LOCATION), "queue": queue}
            )
            logging.info(f"Queue '{queue_id}' created successfully.")
        else:
            logging.exception("Error checking/creating queue:")


def api_endpoint(request):
    """
    Cloud Function 1: Receives incoming requests (URLs) and queues them to Cloud Tasks.
    Ensures the URL is a GCS path to an MP4 video file.

    Args:
        request: Flask request object.
    Returns:
        HTTP response.
    """
    try:
        # Ensure the queue exists
        create_queue()

        request_json = request.get_json()
        if request_json and "url" in request_json:
            url = request_json["url"]

            # URL validation: GCS path, mp4 extension
            if not url.startswith("gs://"):
                return "Error: URL must be a GCS path (start with gs://).", 400
            if not url.endswith(".mp4"):
                return "Error: URL must point to an MP4 video file.", 400

            task_name = create_cloud_task(url)
            return f"URL queued for processing. Task name: {task_name}", 202  # 202 Accepted
        else:
            return "Error: No URL provided in request body.", 400
    except Exception as e:
        logging.exception("Error in api_endpoint:")
        return f"Error: {e}", 500


def api_call_worker(request):
    """
    Cloud Function 2: Executes the API call when triggered by Cloud Tasks.

    Args:
        request: Flask request object containing the URL.
    Returns:
        HTTP response containing the result of the API call.
    """
    video_url = None  # Initialize video_url to None
    try:
        request_json = request.get_json()
        if request_json and "url" in request_json:
            video_url = request_json["url"]  # Assign video_url from request
            result = call_gemini(video_url)  # Call your API function
            return result, 200  # Or a more structured response
        else:
            return "Error: No URL provided in request body.", 400
    except Exception as e:
        logging.exception(f"Error in api_call_worker for URL: {video_url}:")
        return f"Non-Gemini Error for URL: {video_url}. Error: {e}", 500