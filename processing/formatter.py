"""Format transcript JSON into human-readable Markdown."""

import json
from datetime import datetime
from pathlib import Path


def _format_timestamp(seconds):
    """Format seconds into [M:SS] or [H:MM:SS]."""
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"[{h}:{m:02d}:{s:02d}]"
    return f"[{m}:{s:02d}]"


def format_transcript(transcript_data, config):
    """Transform transcript JSON into a Markdown string."""
    filename = transcript_data.get('filename', 'Unknown')
    transcribed_at = transcript_data.get('transcribed_at', '')
    model = transcript_data.get('model', 'unknown')
    service = transcript_data.get('service', 'whisper')
    qc = transcript_data.get('qc', {})

    # Format transcribed_at for display
    try:
        dt = datetime.fromisoformat(transcribed_at)
        transcribed_display = dt.strftime('%Y-%m-%d %H:%M')
    except (ValueError, TypeError):
        transcribed_display = transcribed_at

    # QC info
    score = qc.get('score', 'N/A')
    passed = qc.get('passed', None)
    if passed is not None:
        qc_display = f"{score} ({'PASS' if passed else 'FAIL'})"
    else:
        qc_display = 'Not scored'

    # Header
    lines = [
        f"# Transcript: {filename}",
        "",
        f"- **Source file:** {filename}",
        f"- **Transcribed:** {transcribed_display}",
        f"- **Model:** {service}/{model}",
        f"- **Quality score:** {qc_display}",
        "",
        "---",
        "",
    ]

    # Build paragraphs from segments, stripping bad ones
    segments = transcript_data.get('segments', [])
    bad_ids = set(qc.get('bad_segment_ids', []))
    gap_threshold = 4.0  # seconds between segments to insert paragraph break

    paragraph_texts = []
    current_paragraph = []
    paragraph_start_time = None
    prev_end = None

    for seg in segments:
        if seg['id'] in bad_ids:
            continue

        text = seg.get('text', '').strip()
        if not text:
            continue

        start = seg.get('start', 0)

        # Start a new paragraph on large gap
        if prev_end is not None and (start - prev_end) >= gap_threshold and current_paragraph:
            paragraph_texts.append((paragraph_start_time, ' '.join(current_paragraph)))
            current_paragraph = []
            paragraph_start_time = None

        if paragraph_start_time is None:
            paragraph_start_time = start

        current_paragraph.append(text)
        prev_end = seg.get('end', start)

    # Flush last paragraph
    if current_paragraph:
        paragraph_texts.append((paragraph_start_time, ' '.join(current_paragraph)))

    for start_time, text in paragraph_texts:
        ts = _format_timestamp(start_time)
        lines.append(f"**{ts}** {text}")
        lines.append("")

    return '\n'.join(lines)


def write_readable(transcript_path, transcript_data, config, logger):
    """Write a human-readable .md file to the readable_dir."""
    readable_dir = Path(config.get('transcription', {}).get('readable_dir', 'data/readable'))
    readable_dir.mkdir(parents=True, exist_ok=True)

    md_content = format_transcript(transcript_data, config)

    stem = Path(transcript_path).stem
    md_path = readable_dir / f"{stem}.md"

    with open(md_path, 'w') as f:
        f.write(md_content)

    logger.info(f"Readable transcript written: {md_path.name}")
    return md_path
