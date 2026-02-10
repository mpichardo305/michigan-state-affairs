#!/usr/bin/env python3
"""
Michigan House Hearing Video Transcriber
Orchestrator - scrape, download, and transcribe
"""

import json
import argparse
import sys
from datetime import date
from pathlib import Path

from utils.logging import setup_logging
from utils.lock import FileLock
from utils.notifications import send_notification
from state.manager import StateManager
from scrapers import house as house_scraper
from processing import downloader, transcriber


def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)


def run(config, state_manager, logger, force=False):
    """Main pipeline: scrape -> download -> transcribe"""
    video_dir = Path(config['download']['output_dir'])
    transcript_dir = Path(config['transcription']['output_dir'])
    video_dir.mkdir(parents=True, exist_ok=True)
    transcript_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        'discovered': 0,
        'downloaded': 0,
        'transcribed': 0,
        'skipped': 0,
        'failed': 0
    }

    # 1. Scrape
    videos = house_scraper.scrape(config, logger)
    stats['discovered'] = len(videos)

    # 2. Filter by date and bulk download (no state writes during this phase)
    after_date_str = config['download'].get('after_date')
    after_date = date.fromisoformat(after_date_str) if after_date_str else None

    eligible = []
    for video in videos:
        if after_date and video.get('date') != 'Unknown':
            video_date = date.fromisoformat(video['date'])
            if video_date <= after_date:
                stats['skipped'] += 1
                continue
        eligible.append(video)

    logger.info(f"Eligible for download: {len(eligible)} of {len(videos)} (after {after_date_str or 'all'})")

    for i, video in enumerate(eligible, 1):
        filename = video['filename']
        logger.info(f"[{i}/{len(eligible)}] Processing: {filename}")

        current_state = state_manager.get_state(filename)
        if not force and current_state in ('downloaded', 'transcribed'):
            logger.info(f"Skipping (already {current_state}): {filename}")
            stats['skipped'] += 1
            continue

        try:
            downloader.download(video, video_dir, config, logger)
            stats['downloaded'] += 1
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            stats['failed'] += 1

    # Batch state update: mark all successfully downloaded videos at once
    for video in eligible:
        filename = video['filename']
        if (video_dir / filename).exists() and state_manager.get_state(filename) not in ('downloaded', 'transcribed'):
            state_manager.set_state(filename, 'downloaded', {
                'category': video['category'],
                'date': video['date'],
                'local_path': str(video_dir / filename),
            })

    # 3. Transcribe pending
    transcribed, failed = transcriber.transcribe_pending(
        video_dir, transcript_dir, config, state_manager, logger
    )
    stats['transcribed'] = transcribed
    stats['failed'] += failed

    # 4. Summary
    logger.info("=" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Discovered:  {stats['discovered']}")
    logger.info(f"Eligible:    {len(eligible)}")
    logger.info(f"Downloaded:  {stats['downloaded']}")
    logger.info(f"Transcribed: {stats['transcribed']}")
    logger.info(f"Skipped:     {stats['skipped']}")
    logger.info(f"Failed:      {stats['failed']}")
    logger.info("=" * 70)

    # 5. Notify
    summary = (
        f"<b>Michigan House Transcriber</b>\n\n"
        f"Discovered: {stats['discovered']}\n"
        f"Eligible: {len(eligible)}\n"
        f"Downloaded: {stats['downloaded']}\n"
        f"Transcribed: {stats['transcribed']}\n"
        f"Skipped: {stats['skipped']}\n"
        f"Failed: {stats['failed']}"
    )
    send_notification(summary, config, logger)

    return stats['failed'] == 0


def main():
    parser = argparse.ArgumentParser(
        description='Michigan House Hearing Video Transcriber'
    )
    parser.add_argument(
        '--config', default='config.json',
        help='Path to config file (default: config.json)'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode - do not update execution log'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Force reprocessing of all videos'
    )

    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config)

    logger.info("=" * 70)
    logger.info("Michigan House Transcriber initialized")
    if args.test:
        logger.info("TEST MODE: Execution log will NOT be updated")
    if args.force:
        logger.info("FORCE MODE: Will reprocess all videos")
    logger.info("=" * 70)

    state_manager = StateManager(
        config['execution_log'],
        test_mode=args.test
    )

    lock = FileLock(config['lock_file'], logger)
    try:
        with lock:
            success = run(config, state_manager, logger, force=args.force)
    except RuntimeError:
        logger.error("Could not acquire lock - another instance running?")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
