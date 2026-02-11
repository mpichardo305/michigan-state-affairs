"""Quality control scoring for Whisper transcripts."""

import json
from pathlib import Path


def score_segment(segment, thresholds):
    """Score a single segment against QC thresholds. Returns list of issue strings."""
    issues = []

    no_speech = segment.get('no_speech_prob', 0)
    avg_logprob = segment.get('avg_logprob', 0)
    compression = segment.get('compression_ratio', 1)
    temperature = segment.get('temperature', 0)

    if no_speech >= thresholds.get('no_speech_prob_min', 0.6):
        issues.append('silence_hallucination')

    if avg_logprob <= thresholds.get('avg_logprob_max', -0.5):
        issues.append('low_confidence')

    if compression <= thresholds.get('compression_ratio_max', 0.5):
        issues.append('repetitive')

    if temperature >= thresholds.get('temperature_min', 1.0):
        issues.append('high_temperature')

    return issues


def score_transcript(transcript_data, config):
    """Score an entire transcript. Returns QC result dict."""
    qc_config = config.get('qc', {})
    seg_thresholds = qc_config.get('bad_segment', {})
    fail_thresholds = qc_config.get('fail_thresholds', {})

    segments = transcript_data.get('segments', [])
    total = len(segments)

    if total == 0:
        return {
            'passed': False,
            'score': 0.0,
            'total_segments': 0,
            'bad_segments': 0,
            'issues': ['no_segments'],
            'bad_segment_ids': [],
        }

    bad_ids = []
    all_issues = set()

    for seg in segments:
        issues = score_segment(seg, seg_thresholds)
        if issues:
            bad_ids.append(seg['id'])
            all_issues.update(issues)

    bad_count = len(bad_ids)
    score = round(1 - (bad_count / total), 2)

    # Check wrong language (forced "en" should prevent this, but check anyway)
    detected_lang = transcript_data.get('language', 'en')
    if fail_thresholds.get('wrong_language', True) and detected_lang != 'en':
        all_issues.add('wrong_language')

    # Determine pass/fail
    bad_pct_threshold = fail_thresholds.get('bad_segment_pct', 0.5)
    bad_pct = bad_count / total

    passed = True
    if bad_pct >= bad_pct_threshold:
        passed = False
    if 'wrong_language' in all_issues:
        passed = False

    return {
        'passed': passed,
        'score': score,
        'total_segments': total,
        'bad_segments': bad_count,
        'issues': sorted(all_issues),
        'bad_segment_ids': bad_ids,
    }


def run_qc(transcript_path, config, logger):
    """Score a transcript and write the QC result back into the JSON file."""
    transcript_path = Path(transcript_path)
    logger.info(f"Running QC on: {transcript_path.name}")

    with open(transcript_path, 'r') as f:
        transcript_data = json.load(f)

    result = score_transcript(transcript_data, config)

    status = 'PASS' if result['passed'] else 'FAIL'
    logger.info(
        f"QC {status}: {transcript_path.name} "
        f"(score={result['score']}, bad={result['bad_segments']}/{result['total_segments']}"
        f"{', issues=' + ','.join(result['issues']) if result['issues'] else ''})"
    )

    transcript_data['qc'] = result

    with open(transcript_path, 'w') as f:
        json.dump(transcript_data, f, indent=2)

    return result
