import json
from datetime import datetime
from pathlib import Path

import whisper


def transcribe(video_path, filename, transcript_dir, config, logger):
    """Transcribe a single video using Whisper. Returns the transcript Path."""
    transcript_file = Path(transcript_dir) / f"{video_path.stem}.json"

    if transcript_file.exists():
        logger.info(f"Transcript already exists: {filename}")
        return transcript_file

    logger.info(f"Transcribing: {filename}")

    try:
        model_name = config['transcription']['model']
        logger.info(f"Loading Whisper model: {model_name}")
        model = whisper.load_model(model_name)

        logger.info("Running transcription (this may take a while)...")
        result = model.transcribe(str(video_path))

        transcript_data = {
            'filename': filename,
            'transcribed_at': datetime.now().isoformat(),
            'service': 'whisper',
            'model': model_name,
            'text': result['text'],
            'segments': result.get('segments', []),
            'language': result.get('language')
        }

        with open(transcript_file, 'w') as f:
            json.dump(transcript_data, f, indent=2)

        logger.info(f"Transcription complete: {filename}")
        return transcript_file

    except Exception as e:
        logger.error(f"Transcription failed for {filename}: {e}")
        if transcript_file.exists():
            transcript_file.unlink()
        raise


def transcribe_pending(video_dir, transcript_dir, config, state_manager, logger):
    """Transcribe any downloaded videos that are missing transcripts."""
    logger.info("Checking for videos pending transcription...")

    video_files = sorted(Path(video_dir).glob("*.mp4"))
    pending = []

    for video_path in video_files:
        transcript_file = Path(transcript_dir) / f"{video_path.stem}.json"
        if not transcript_file.exists():
            pending.append(video_path)

    if not pending:
        logger.info("No videos pending transcription")
        return 0, 0

    logger.info(f"Found {len(pending)} videos pending transcription")
    transcribed = 0
    failed = 0

    for i, video_path in enumerate(pending, 1):
        filename = video_path.name
        logger.info(f"[{i}/{len(pending)}] Transcribing: {filename}")

        state_manager.set_state(filename, 'transcribing')

        try:
            transcript_path = transcribe(video_path, filename, transcript_dir, config, logger)
            state_manager.set_state(filename, 'transcribed', {
                'transcript_path': str(transcript_path)
            })
            transcribed += 1

        except Exception as e:
            logger.error(f"Failed to transcribe {filename}: {e}")
            state_manager.set_state(filename, 'failed', {
                'error': str(e)
            })
            failed += 1

    logger.info(f"Transcription catch-up complete: {transcribed} transcribed, {failed} failed")
    return transcribed, failed
