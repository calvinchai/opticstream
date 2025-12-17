import glob
import os
import shutil
import time

from prefect import flow, get_run_logger, task
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration
WATCH_DIRECTORIES = "/space/zircon/5/users/kchai/I55_slice*/analysis/"
FILE_PATTERN = "*.jpg"
SLACK_BOT_TOKEN = 'xoxb-5890633520343-9932310181297-biQaPdHnAP3jlE0uxhkB2RSF'
SLACK_CHANNEL_ID = "C09SA78A8QJ"  # Slack channel ID


@task(name="Upload to Slack", retries=2, retry_delay_seconds=5)
def upload_image_to_slack(filepath):
    logger = get_run_logger()
    client = WebClient(token=SLACK_BOT_TOKEN)
    filename = os.path.basename(filepath)

    try:
        logger.info(f"Uploading: {filename}")

        response = client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            file=filepath,
            title=f"New Image: {filename}",
            initial_comment=f"Detected new file: {filename}"
        )
        logger.info(f"Uploaded {filename}")
        return True

    except SlackApiError as e:
        logger.error(f"Slack upload error: {e.response['error']}")
        raise


@task(name="Archive File")
def move_to_processed(filepath):
    logger = get_run_logger()
    filename = os.path.basename(filepath)

    base_dir = os.path.dirname(filepath)
    processed_dir = os.path.join(base_dir, "processed")

    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    destination = os.path.join(processed_dir, filename)

    try:
        shutil.move(filepath, destination)
        logger.info(f"Moved {filename} → {processed_dir}")
    except Exception as e:
        logger.error(f"Move failed: {str(e)}")
        raise


@flow(name="3D Axis Watcher", log_prints=True)
def folder_watch_flow():
    logger = get_run_logger()
    logger.info("Watching directories:")
    for d in glob.glob(WATCH_DIRECTORIES):
        logger.info(f"  {d}")

    # Infinite loop
    while True:
        for watch_dir in glob.glob(WATCH_DIRECTORIES):

            search_path = os.path.join(watch_dir, FILE_PATTERN)
            files_found = glob.glob(search_path)

            if files_found:
                logger.info(f"[{watch_dir}] Found {len(files_found)} new files")

                for filepath in files_found:
                    uploaded = upload_image_to_slack(filepath)
                    if uploaded:
                        move_to_processed(filepath)

        time.sleep(10)


if __name__ == "__main__":
    folder_watch_flow.serve(
        name="slack-watcher-deployment",
        tags=["file-watcher"],
        description="Watches multiple slice analysis folders and uploads images to "
        "Slack"
    )
