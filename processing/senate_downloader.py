import subprocess
from pathlib import Path


def download(video, video_dir, config, logger):
    """Download a Senate HLS video as MP4 via ffmpeg. Returns the output Path."""
    filename = video['filename']
    output_path = Path(video_dir) / filename

    if output_path.exists():
        logger.info(f"Video already exists: {filename}")
        return output_path

    hls_url = video['hls_url']
    logger.info(f"Downloading HLS stream: {filename} from {hls_url}")

    try:
        _download_hls_video(hls_url, output_path, config, logger)
        size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Downloaded: {filename} ({size_mb:.1f} MB)")
        return output_path

    except Exception as e:
        logger.error(f"Download failed for {filename}: {e}")
        if output_path.exists():
            output_path.unlink()
        raise


def _download_hls_video(hls_url, output_path, config, logger):
    """Download HLS stream to MP4 using ffmpeg."""
    timeout = config['download']['timeout']

    result = subprocess.run(
        [
            'ffmpeg',
            '-i', hls_url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-y',
            str(output_path),
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed (exit {result.returncode}): {result.stderr[-500:]}")
