#!/usr/bin/env python3
"""
Michigan State Affairs Video Transcriber
Orchestrator - scrape, download, and transcribe House and Senate hearings
"""

import json
import argparse
import os
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from utils.logging import setup_logging
from utils.lock import FileLock
from utils.notifications import send_notification
from state.manager import StateManager
from scrapers import house as house_scraper
from scrapers import senate as senate_scraper
from processing import downloader, senate_downloader, transcriber, s3_uploader
from processing.qc import run_qc
from processing.grammar import write_final


def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)


def _filter_by_date(videos, config, logger):
    """Filter videos by after_date config. Returns (eligible, skipped_count)."""
    after_date_str = config['download'].get('after_date')
    after_date = date.fromisoformat(after_date_str) if after_date_str else None

    eligible = []
    skipped = 0
    for video in videos:
        if after_date and video.get('date') != 'Unknown':
            video_date = date.fromisoformat(video['date'])
            if video_date <= after_date:
                skipped += 1
                continue
        eligible.append(video)

    logger.info(f"Eligible for download: {len(eligible)} of {len(videos)} (after {after_date_str or 'all'})")
    return eligible, skipped


def _process_video(video, video_dir, transcript_dir, download_fn, config, state_manager, logger, metadata):
    """Download, transcribe, and delete a single video. Returns 'transcribed' or 'failed'."""
    filename = video['filename']
    video_path = video_dir / filename

    try:
        download_fn(video, video_dir, config, logger)
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")
        return 'failed'

    state_manager.set_state(filename, 'downloaded', metadata)

    # Start S3 upload in background (parallel with transcription)
    upload_result = None
    s3_enabled = config.get('s3', {}).get('enabled', False)
    if s3_enabled and video_path.exists():
        upload_result = s3_uploader.start_upload_thread(video_path, config, logger)

    try:
        state_manager.set_state(filename, 'transcribing')
        transcript_path = transcriber.transcribe(video_path, filename, transcript_dir, config, logger)

        # Run QC scoring
        qc_result = run_qc(transcript_path, config, logger)

        # Write human-readable Markdown
        with open(transcript_path, 'r') as f:
            transcript_data = json.load(f)
        write_final(transcript_path, transcript_data, config, logger)

        transcribed_metadata = {
            'transcript_path': str(transcript_path),
            'qc_passed': qc_result['passed'],
            'qc_score': qc_result['score'],
        }

        # Upload final transcript to S3
        if s3_enabled:
            final_md = Path(config['transcription']['final_dir']) / f"{transcript_path.stem}.md"
            if final_md.exists():
                try:
                    t_s3_key = s3_uploader.upload(final_md, config, logger, prefix='transcripts/')
                    transcribed_metadata['transcript_s3_key'] = t_s3_key
                except Exception as e:
                    logger.error(f"S3 transcript upload failed for {final_md.name}: {e}")
    except Exception as e:
        logger.error(f"Failed to transcribe {filename}: {e}")
        state_manager.set_state(filename, 'failed', {'error': str(e)})
        return 'failed'

    # Wait for S3 upload before deleting local file
    if upload_result is not None:
        s3_key, s3_error = s3_uploader.wait_for_upload(upload_result)
        if s3_key:
            transcribed_metadata['s3_key'] = s3_key
        else:
            logger.error(f"S3 upload failed for {filename}, keeping local file: {s3_error}")

    state_manager.set_state(filename, 'transcribed', transcribed_metadata)

    # Delete MP4 after successful transcription + upload to free disk space
    can_delete = upload_result is None or upload_result['s3_key'] is not None
    if can_delete and video_path.exists():
        size_mb = video_path.stat().st_size / 1024 / 1024
        video_path.unlink()
        logger.info(f"Deleted video after transcription: {filename} ({size_mb:.1f} MB freed)")

    return 'transcribed'


def _cleanup_transcribed_videos(video_dir, transcript_dir, config, logger):
    """Upload to S3 (if enabled) then delete any MP4s that already have a transcript JSON."""
    video_dir = Path(video_dir)
    transcript_dir = Path(transcript_dir)
    s3_enabled = config.get('s3', {}).get('enabled', False)
    freed = 0
    for video_path in video_dir.glob("*.mp4"):
        transcript_file = transcript_dir / f"{video_path.stem}.json"
        if transcript_file.exists():
            # Upload to S3 before deleting (sequential — no transcription to parallelize with)
            if s3_enabled:
                try:
                    s3_uploader.upload(video_path, config, logger)
                except Exception as e:
                    logger.error(f"Cleanup: S3 upload failed for {video_path.name}, keeping local file: {e}")
                    continue
            size_mb = video_path.stat().st_size / 1024 / 1024
            video_path.unlink()
            freed += size_mb
            logger.info(f"Cleanup: deleted {video_path.name} ({size_mb:.1f} MB)")
    if freed > 0:
        logger.info(f"Cleanup complete: {freed:.1f} MB freed")


def run_house(config, state_manager, logger, force=False):
    """House pipeline: scrape -> download -> transcribe"""
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

    # 2. Filter and download
    eligible, skipped = _filter_by_date(videos, config, logger)
    stats['skipped'] += skipped

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

    # Batch state update
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

    return stats


def run_senate(config, state_manager, logger, force=False):
    """Senate pipeline: scrape -> HLS download -> transcribe"""
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
    videos = senate_scraper.scrape(config, logger)
    stats['discovered'] = len(videos)

    # 2. Filter and download
    eligible, skipped = _filter_by_date(videos, config, logger)
    stats['skipped'] += skipped

    for i, video in enumerate(eligible, 1):
        filename = video['filename']
        logger.info(f"[{i}/{len(eligible)}] Processing: {filename}")

        current_state = state_manager.get_state(filename)
        if not force and current_state in ('downloaded', 'transcribed'):
            logger.info(f"Skipping (already {current_state}): {filename}")
            stats['skipped'] += 1
            continue

        try:
            senate_downloader.download(video, video_dir, config, logger)
            stats['downloaded'] += 1
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            stats['failed'] += 1

    # Batch state update
    for video in eligible:
        filename = video['filename']
        if (video_dir / filename).exists() and state_manager.get_state(filename) not in ('downloaded', 'transcribed'):
            state_manager.set_state(filename, 'downloaded', {
                'title': video.get('title', ''),
                'date': video['date'],
                'local_path': str(video_dir / filename),
                'source': 'senate',
                'hls_url': video.get('hls_url', ''),
            })

    # 3. Transcribe pending
    transcribed, failed = transcriber.transcribe_pending(
        video_dir, transcript_dir, config, state_manager, logger
    )
    stats['transcribed'] = transcribed
    stats['failed'] += failed

    return stats


def run_house_streaming(config, state_manager, logger, force=False):
    """House pipeline: scrape -> (download + transcribe + cleanup) per video"""
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

    videos = house_scraper.scrape(config, logger)
    stats['discovered'] = len(videos)

    eligible, skipped = _filter_by_date(videos, config, logger)
    stats['skipped'] += skipped

    for i, video in enumerate(eligible, 1):
        filename = video['filename']
        logger.info(f"[{i}/{len(eligible)}] Processing: {filename}")

        current_state = state_manager.get_state(filename)
        if not force and current_state in ('downloaded', 'transcribed'):
            logger.info(f"Skipping (already {current_state}): {filename}")
            stats['skipped'] += 1
            continue

        metadata = {
            'category': video['category'],
            'date': video['date'],
            'local_path': str(video_dir / filename),
        }
        result = _process_video(
            video, video_dir, transcript_dir, downloader.download,
            config, state_manager, logger, metadata
        )
        if result == 'transcribed':
            stats['downloaded'] += 1
            stats['transcribed'] += 1
        else:
            stats['failed'] += 1

    # Catch-up: transcribe+delete any orphaned videos from previous runs
    transcribed, failed = transcriber.transcribe_pending(
        video_dir, transcript_dir, config, state_manager, logger
    )
    stats['transcribed'] += transcribed
    stats['failed'] += failed
    _cleanup_transcribed_videos(video_dir, transcript_dir, config, logger)

    return stats


def run_senate_streaming(config, state_manager, logger, force=False):
    """Senate pipeline: scrape -> (HLS download + transcribe + cleanup) per video"""
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

    videos = senate_scraper.scrape(config, logger)
    stats['discovered'] = len(videos)

    eligible, skipped = _filter_by_date(videos, config, logger)
    stats['skipped'] += skipped

    for i, video in enumerate(eligible, 1):
        filename = video['filename']
        logger.info(f"[{i}/{len(eligible)}] Processing: {filename}")

        current_state = state_manager.get_state(filename)
        if not force and current_state in ('downloaded', 'transcribed'):
            logger.info(f"Skipping (already {current_state}): {filename}")
            stats['skipped'] += 1
            continue

        metadata = {
            'title': video.get('title', ''),
            'date': video['date'],
            'local_path': str(video_dir / filename),
            'source': 'senate',
            'hls_url': video.get('hls_url', ''),
        }
        result = _process_video(
            video, video_dir, transcript_dir, senate_downloader.download,
            config, state_manager, logger, metadata
        )
        if result == 'transcribed':
            stats['downloaded'] += 1
            stats['transcribed'] += 1
        else:
            stats['failed'] += 1

    # Catch-up: transcribe+delete any orphaned videos from previous runs
    transcribed, failed = transcriber.transcribe_pending(
        video_dir, transcript_dir, config, state_manager, logger
    )
    stats['transcribed'] += transcribed
    stats['failed'] += failed
    _cleanup_transcribed_videos(video_dir, transcript_dir, config, logger)

    return stats


def _log_summary(label, stats, eligible_count, logger):
    """Log execution summary for a source."""
    logger.info(f"--- {label} ---")
    logger.info(f"Discovered:  {stats['discovered']}")
    logger.info(f"Eligible:    {eligible_count}")
    logger.info(f"Downloaded:  {stats['downloaded']}")
    logger.info(f"Transcribed: {stats['transcribed']}")
    logger.info(f"Skipped:     {stats['skipped']}")
    logger.info(f"Failed:      {stats['failed']}")


def run(config, state_manager, logger, source='all', force=False):
    """Run pipeline for selected source(s)."""
    success = True
    all_stats = []

    if source in ('house', 'all'):
        logger.info("=" * 70)
        logger.info("HOUSE PIPELINE")
        logger.info("=" * 70)
        stats = run_house_streaming(config, state_manager, logger, force)
        all_stats.append(('House', stats))
        if stats['failed'] > 0:
            success = False

    if source in ('senate', 'all'):
        logger.info("=" * 70)
        logger.info("SENATE PIPELINE")
        logger.info("=" * 70)
        stats = run_senate_streaming(config, state_manager, logger, force)
        all_stats.append(('Senate', stats))
        if stats['failed'] > 0:
            success = False

    # Summary
    logger.info("=" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 70)
    for label, stats in all_stats:
        _log_summary(label, stats, stats['discovered'] - stats['skipped'], logger)
    logger.info("=" * 70)

    # Notify
    lines = [f"<b>Michigan State Affairs Transcriber</b>\n"]
    for label, stats in all_stats:
        lines.append(
            f"<b>{label}</b>: "
            f"{stats['downloaded']} downloaded, "
            f"{stats['transcribed']} transcribed, "
            f"{stats['failed']} failed"
        )
    send_notification('\n'.join(lines), config, logger)

    return success


def qc_existing(config, state_manager, logger):
    """Run QC + formatting on all existing transcripts without re-downloading."""
    transcript_dir = Path(config['transcription']['output_dir'])
    transcripts = sorted(transcript_dir.glob("*.json"))

    if not transcripts:
        logger.info("No existing transcripts found")
        return

    s3_enabled = config.get('s3', {}).get('enabled', False)
    logger.info(f"Running QC on {len(transcripts)} existing transcripts")
    passed = 0
    failed = 0

    for transcript_path in transcripts:
        qc_result = run_qc(transcript_path, config, logger)

        with open(transcript_path, 'r') as f:
            transcript_data = json.load(f)
        write_final(transcript_path, transcript_data, config, logger)

        # Upload final transcript to S3
        if s3_enabled:
            final_md = Path(config['transcription']['final_dir']) / f"{transcript_path.stem}.md"
            if final_md.exists():
                try:
                    s3_uploader.upload(final_md, config, logger, prefix='transcripts/')
                except Exception as e:
                    logger.error(f"S3 transcript upload failed for {final_md.name}: {e}")

        # Update state manager metadata if entry exists
        filename = transcript_path.stem + '.mp4'
        entry = state_manager.get_entry(filename)
        if entry:
            state_manager.set_state(filename, entry.get('state', 'transcribed'), {
                'qc_passed': qc_result['passed'],
                'qc_score': qc_result['score'],
            })

        if qc_result['passed']:
            passed += 1
        else:
            failed += 1

    logger.info(f"QC complete: {passed} passed, {failed} failed out of {len(transcripts)}")


def retranscribe(config, state_manager, logger):
    """Re-download and re-transcribe any transcripts that failed QC."""
    transcript_dir = Path(config['transcription']['output_dir'])
    video_dir = Path(config['download']['output_dir'])
    video_dir.mkdir(parents=True, exist_ok=True)
    transcript_dir.mkdir(parents=True, exist_ok=True)

    # Collect from two sources: QC-failed transcript files + state manager entries
    filenames_to_redo = set()

    # Source 1: transcript JSONs with qc.passed == false
    for transcript_path in sorted(transcript_dir.glob("*.json")):
        with open(transcript_path, 'r') as f:
            data = json.load(f)
        qc = data.get('qc', {})
        if not qc.get('passed', True):
            filenames_to_redo.add(transcript_path.stem + '.mp4')

    # Source 2: state manager entries with qc_passed == false
    for filename, entry in state_manager.data.get('videos', {}).items():
        if entry.get('qc_passed') is False:
            filenames_to_redo.add(filename)

    if not filenames_to_redo:
        logger.info("No failed transcripts to re-process")
        return

    filenames_to_redo = sorted(filenames_to_redo)
    logger.info(f"Found {len(filenames_to_redo)} transcripts that failed QC — re-transcribing")
    success = 0
    failed = 0

    for filename in filenames_to_redo:
        video_path = video_dir / filename
        transcript_path = transcript_dir / f"{Path(filename).stem}.json"
        entry = state_manager.get_entry(filename)
        is_senate = (entry.get('source') == 'senate') if entry else filename.startswith('senate_')

        # Remove old transcript if it exists
        if transcript_path.exists():
            logger.info(f"Removing old transcript: {transcript_path.name}")
            transcript_path.unlink()

        # Download video
        try:
            if is_senate:
                hls_url = entry.get('hls_url') if entry else None
                if not hls_url:
                    logger.error(f"No HLS URL stored for {filename} — cannot re-download")
                    failed += 1
                    continue
                senate_downloader.download({'filename': filename, 'hls_url': hls_url}, video_dir, config, logger)
            else:
                downloader.download({'filename': filename}, video_dir, config, logger)
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            state_manager.set_state(filename, 'failed', {'error': str(e)})
            failed += 1
            continue

        # Start S3 upload in background (parallel with transcription)
        upload_result = None
        s3_enabled = config.get('s3', {}).get('enabled', False)
        if s3_enabled and video_path.exists():
            upload_result = s3_uploader.start_upload_thread(video_path, config, logger)

        # Transcribe
        try:
            state_manager.set_state(filename, 'transcribing')
            new_transcript_path = transcriber.transcribe(video_path, filename, transcript_dir, config, logger)

            qc_result = run_qc(new_transcript_path, config, logger)

            with open(new_transcript_path, 'r') as f:
                transcript_data = json.load(f)
            write_final(new_transcript_path, transcript_data, config, logger)

            transcribed_metadata = {
                'transcript_path': str(new_transcript_path),
                'qc_passed': qc_result['passed'],
                'qc_score': qc_result['score'],
            }

            # Upload final transcript to S3
            if s3_enabled:
                final_md = Path(config['transcription']['final_dir']) / f"{new_transcript_path.stem}.md"
                if final_md.exists():
                    try:
                        t_s3_key = s3_uploader.upload(final_md, config, logger, prefix='transcripts/')
                        transcribed_metadata['transcript_s3_key'] = t_s3_key
                    except Exception as e:
                        logger.error(f"S3 transcript upload failed for {final_md.name}: {e}")
        except Exception as e:
            logger.error(f"Failed to transcribe {filename}: {e}")
            state_manager.set_state(filename, 'failed', {'error': str(e)})
            failed += 1
            continue

        # Wait for S3 upload before deleting local file
        if upload_result is not None:
            s3_key, s3_error = s3_uploader.wait_for_upload(upload_result)
            if s3_key:
                transcribed_metadata['s3_key'] = s3_key
            else:
                logger.error(f"S3 upload failed for {filename}, keeping local file: {s3_error}")

        state_manager.set_state(filename, 'transcribed', transcribed_metadata)
        success += 1

        # Delete MP4 after transcription + upload
        can_delete = upload_result is None or upload_result['s3_key'] is not None
        if can_delete and video_path.exists():
            size_mb = video_path.stat().st_size / 1024 / 1024
            video_path.unlink()
            logger.info(f"Deleted video: {filename} ({size_mb:.1f} MB freed)")

    logger.info(f"Retranscription complete: {success} succeeded, {failed} failed")


def upload_and_delete_existing(config, state_manager, logger):
    """Upload all local MP4s to S3, record s3_key in state, then delete the local files."""
    video_dir = Path(config['download']['output_dir'])
    mp4s = sorted(video_dir.glob("*.mp4"))

    if not mp4s:
        logger.info("No local MP4 files found to upload")
        return

    logger.info(f"Uploading {len(mp4s)} local MP4(s) to S3")
    uploaded = 0
    freed = 0

    for video_path in mp4s:
        try:
            s3_key = s3_uploader.upload(video_path, config, logger)
            uploaded += 1
            # Record s3_key in state if entry exists
            filename = video_path.name
            entry = state_manager.get_entry(filename)
            if entry:
                state_manager.set_state(filename, entry.get('state', 'transcribed'), {
                    's3_key': s3_key,
                })
            # Delete local file after successful upload
            size_mb = video_path.stat().st_size / 1024 / 1024
            video_path.unlink()
            freed += size_mb
            logger.info(f"Deleted local file: {filename} ({size_mb:.1f} MB freed)")
        except Exception as e:
            logger.error(f"Failed to upload {video_path.name}, keeping local file: {e}")

    logger.info(f"Upload complete: {uploaded}/{len(mp4s)} uploaded, {freed:.1f} MB freed")


def main():
    parser = argparse.ArgumentParser(
        description='Michigan State Affairs Video Transcriber'
    )
    parser.add_argument(
        '--config', default='config.json',
        help='Path to config file (default: config.json)'
    )
    parser.add_argument(
        '--source', choices=['house', 'senate', 'all'], default='all',
        help='Which source to process (default: all)'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode - do not update execution log'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Force reprocessing of all videos'
    )
    parser.add_argument(
        '--qc-existing', action='store_true',
        help='Run QC + formatting on all existing transcripts (no downloading)'
    )
    parser.add_argument(
        '--retranscribe', action='store_true',
        help='Re-download and re-transcribe any transcripts that failed QC'
    )
    parser.add_argument(
        '--upload-and-delete-existing', action='store_true',
        help='Upload all local MP4s to S3 then delete them locally'
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config)

    logger.info("=" * 70)
    logger.info(f"Michigan State Affairs Transcriber initialized (source: {args.source})")
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
            if args.upload_and_delete_existing:
                upload_and_delete_existing(config, state_manager, logger)
            elif args.qc_existing:
                qc_existing(config, state_manager, logger)
            elif args.retranscribe:
                retranscribe(config, state_manager, logger)
            else:
                success = run(config, state_manager, logger, source=args.source, force=args.force)
                sys.exit(0 if success else 1)
    except RuntimeError:
        logger.error("Could not acquire lock - another instance running?")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
