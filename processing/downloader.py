import requests
import urllib3
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


ARCHIVE_BASE_URL = "https://www.house.mi.gov/ArchiveVideoFiles"


def download(video, video_dir, config, logger):
    """Download a video file. Returns the output Path."""
    filename = video['filename']
    output_path = Path(video_dir) / filename

    if output_path.exists():
        logger.info(f"Video already exists: {filename}")
        return output_path

    download_url = f"{ARCHIVE_BASE_URL}/{filename}"
    logger.info(f"Downloading: {filename} from {download_url}")

    try:
        response = requests.get(
            download_url,
            stream=True,
            timeout=config['download']['timeout'],
            verify=False
        )
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        chunk_size = config['download']['chunk_size']

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    if downloaded % (10 * 1024 * 1024) < chunk_size:
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"  Progress: {progress:.1f}%")

        logger.info(f"Downloaded: {filename} ({downloaded / 1024 / 1024:.1f} MB)")
        return output_path

    except Exception as e:
        logger.error(f"Download failed for {filename}: {e}")
        if output_path.exists():
            output_path.unlink()
        raise
