"""Grammar correction for transcripts: punctuate then polish."""

import json
import re
from pathlib import Path

from transformers import pipeline as hf_pipeline
import language_tool_python


# Module-level singletons (heavy to initialize, reuse across calls)
_punct_pipe = None
_lang_tool = None

_PUNCT_MODEL = "oliverguhr/fullstop-punctuation-multilang-large"


def _get_punct_pipe():
    global _punct_pipe
    if _punct_pipe is None:
        _punct_pipe = hf_pipeline("ner", model=_PUNCT_MODEL, aggregation_strategy="none")
    return _punct_pipe


def _get_lang_tool():
    global _lang_tool
    if _lang_tool is None:
        _lang_tool = language_tool_python.LanguageTool('en-US')
    return _lang_tool


def _overlap_chunks(lst, n, stride=0):
    """Yield successive n-sized chunks from lst with stride overlap."""
    for i in range(0, len(lst), n - stride):
        yield lst[i:i + n]


def punctuate(text):
    """Add periods, commas, and question marks using fullstop-punctuation model.

    Replicates deepmultilingualpunctuation logic with the fixed pipeline API.
    """
    pipe = _get_punct_pipe()

    # Strip existing punctuation (except in numbers) so the model can re-predict
    text = re.sub(r"(?<!\d)[.,;:!?](?!\d)", "", text)
    words = text.split()
    if not words:
        return text

    overlap = 5
    chunk_size = 230
    if len(words) <= chunk_size:
        overlap = 0

    batches = list(_overlap_chunks(words, chunk_size, overlap))
    if len(batches[-1]) <= overlap and len(batches) > 1:
        batches.pop()

    tagged_words = []
    for batch in batches:
        # Use last batch completely (no overlap trimming)
        current_overlap = 0 if batch is batches[-1] else overlap
        text_chunk = " ".join(batch)
        result = pipe(text_chunk)

        char_index = 0
        result_index = 0
        for word in batch[:len(batch) - current_overlap]:
            char_index += len(word) + 1
            label = "0"
            while result_index < len(result) and char_index > result[result_index]["end"]:
                label = result[result_index]["entity"]
                result_index += 1
            tagged_words.append((word, label))

    # Rebuild text with punctuation
    parts = []
    for word, label in tagged_words:
        parts.append(word)
        if label in ".,?-:":
            parts.append(label)
        parts.append(" ")

    return "".join(parts).strip()


def polish(text):
    """Fix grammar and capitalization using LanguageTool."""
    tool = _get_lang_tool()
    return tool.correct(text)


def correct_paragraph(text):
    """Run the full two-step correction: punctuate then polish."""
    text = punctuate(text)
    text = polish(text)
    return text


def write_final(transcript_path, transcript_data, config, logger):
    """Write a grammar-corrected .md file to data/transcripts-final/."""
    from processing.formatter import _format_timestamp
    from datetime import datetime

    final_dir = Path(config.get('transcription', {}).get('final_dir', 'data/transcripts-final'))
    final_dir.mkdir(parents=True, exist_ok=True)

    filename = transcript_data.get('filename', 'Unknown')
    transcribed_at = transcript_data.get('transcribed_at', '')
    model = transcript_data.get('model', 'unknown')
    service = transcript_data.get('service', 'whisper')
    qc = transcript_data.get('qc', {})

    try:
        dt = datetime.fromisoformat(transcribed_at)
        transcribed_display = dt.strftime('%Y-%m-%d %H:%M')
    except (ValueError, TypeError):
        transcribed_display = transcribed_at

    score = qc.get('score', 'N/A')
    passed = qc.get('passed', None)
    if passed is not None:
        qc_display = f"{score} ({'PASS' if passed else 'FAIL'})"
    else:
        qc_display = 'Not scored'

    lines = [
        f"# Transcript: {filename}",
        "",
        f"- **Source file:** {filename}",
        f"- **Transcribed:** {transcribed_display}",
        f"- **Model:** {service}/{model}",
        f"- **Quality score:** {qc_display}",
        f"- **Grammar corrected:** Yes (punctuation + polish)",
        "",
        "---",
        "",
    ]

    # Build paragraphs from segments, stripping bad ones
    segments = transcript_data.get('segments', [])
    bad_ids = set(qc.get('bad_segment_ids', []))
    gap_threshold = 4.0

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
        if prev_end is not None and (start - prev_end) >= gap_threshold and current_paragraph:
            paragraph_texts.append((paragraph_start_time, ' '.join(current_paragraph)))
            current_paragraph = []
            paragraph_start_time = None
        if paragraph_start_time is None:
            paragraph_start_time = start
        current_paragraph.append(text)
        prev_end = seg.get('end', start)

    if current_paragraph:
        paragraph_texts.append((paragraph_start_time, ' '.join(current_paragraph)))

    # Correct each paragraph
    total = len(paragraph_texts)
    for i, (start_time, text) in enumerate(paragraph_texts):
        logger.info(f"  Correcting paragraph {i + 1}/{total}")
        corrected = correct_paragraph(text)
        ts = _format_timestamp(start_time)
        lines.append(f"**{ts}** {corrected}")
        lines.append("")

    md_content = '\n'.join(lines)
    stem = Path(transcript_path).stem
    md_path = final_dir / f"{stem}.md"

    with open(md_path, 'w') as f:
        f.write(md_content)

    logger.info(f"Grammar-corrected transcript written: {md_path.name}")
    return md_path
