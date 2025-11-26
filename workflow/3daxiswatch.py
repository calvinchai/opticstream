import os
import time
import glob
import shutil
from prefect import flow, task, get_run_logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration
WATCH_DIRECTORY = "/space/zircon/5/users/kchai/I55_slice12/analysis/"
PROCESSED_DIRECTORY = "/space/zircon/5/users/kchai/I55_slice12/analysis/processed/"
FILE_PATTERN = "*.jpg"
SLACK_BOT_TOKEN = 'xoxb-5890633520343-9932310181297-biQaPdHnAP3jlE0uxhkB2RSF'
SLACK_CHANNEL_ID = "C09SA78A8QJ" # Channel ID (usually starts with C)

@task(name="Upload to Slack", retries=2, retry_delay_seconds=5)
def upload_image_to_slack(filepath):
    """
    Uploads the specified file to Slack using the WebClient.
    """
    logger = get_run_logger()
    client = WebClient(token=SLACK_BOT_TOKEN)
    filename = os.path.basename(filepath)

    try:
        logger.info(f"Attempting to upload {filename}...")
        
        # files_upload_v2 is the current standard for Slack SDK
        response = client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            file=filepath,
            title=f"New Image: {filename}",
            initial_comment=f"Detected new file: {filename}"
        )
        logger.info(f"Successfully uploaded {filename}")
        return True
        
    except SlackApiError as e:
        logger.error(f"Error uploading file: {e.response['error']}")
        raise

@task(name="Archive File")
def move_to_processed(filepath):
    """
    Moves the file to a processed subdirectory so it isn't uploaded again.
    """
    logger = get_run_logger()
    filename = os.path.basename(filepath)
    
    # Ensure processed directory exists
    if not os.path.exists(PROCESSED_DIRECTORY):
        os.makedirs(PROCESSED_DIRECTORY)
        
    destination = os.path.join(PROCESSED_DIRECTORY, filename)
    
    try:
        shutil.move(filepath, destination)
        logger.info(f"Moved {filename} to {PROCESSED_DIRECTORY}")
    except Exception as e:
        logger.error(f"Failed to move file: {str(e)}")
        raise

@flow(name="3D Axis Watcher", log_prints=True)
def folder_watch_flow():
    """
    Continuous loop flow that watches a folder for specific images.
    """
    logger = get_run_logger()
    logger.info(f"Starting watch on {WATCH_DIRECTORY} for pattern {FILE_PATTERN}")

    # Ensure the watch directory exists
    if not os.path.exists(WATCH_DIRECTORY):
        os.makedirs(WATCH_DIRECTORY)

    # Infinite loop to act as a service
    while True:
        # specific glob pattern for 3daxis*.jpg
        search_path = os.path.join(WATCH_DIRECTORY, FILE_PATTERN)
        files_found = glob.glob(search_path)

        if files_found:
            logger.info(f"Found {len(files_found)} new files.")
            
            for filepath in files_found:
                # 1. Upload the file
                upload_success = upload_image_to_slack(filepath)
                
                # 2. If upload succeeded, move the file so we don't upload it again next loop
                if upload_success:
                    move_to_processed(filepath)
        
        # Wait for 10 seconds before checking again
        time.sleep(10)

if __name__ == "__main__":
    folder_watch_flow.serve(
        name="slack-watcher-deployment",
        tags=["file-watcher"],
        description="Long-running service that uploads images to Slack"
    )