"""S3 upload for video files — runs in a background thread concurrent with transcription."""

import os
import threading

import boto3
from botocore.exceptions import ClientError


def upload(video_path, config, logger):
    """Upload a video file to S3. Returns the S3 key on success, raises on failure."""
    s3_config = config.get('s3', {})
    prefix = s3_config.get('prefix', 'videos/')

    bucket = os.environ['AWS_S3_BUCKET']
    region = os.environ.get('AWS_REGION', 'us-east-2')

    s3_key = f"{prefix}{video_path.name}"

    logger.info(f"S3 upload starting: {video_path.name} → s3://{bucket}/{s3_key}")

    client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    client.upload_file(str(video_path), bucket, s3_key)

    logger.info(f"S3 upload complete: {video_path.name}")
    return s3_key


def start_upload_thread(video_path, config, logger):
    """Kick off S3 upload in a background thread. Returns a result holder dict."""
    result = {'s3_key': None, 'error': None, 'done': threading.Event()}

    def _run():
        try:
            result['s3_key'] = upload(video_path, config, logger)
        except Exception as e:
            result['error'] = e
            logger.error(f"S3 upload failed for {video_path.name}: {e}")
        finally:
            result['done'].set()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    result['thread'] = thread
    return result


def wait_for_upload(result_holder):
    """Block until the upload thread finishes. Returns (s3_key, error)."""
    result_holder['done'].wait()
    return result_holder['s3_key'], result_holder['error']
