# Michigan House Transcriber - Developer Notes

## Project Structure

```
michigan-house-hearings/
├── main.py                  # Orchestrator + CLI entry point
├── config.json
├── requirements.txt
├── run_transcriber.sh       # Wrapper: activates venv, runs main.py
├── scrapers/
│   └── house.py             # Selenium scraping for house.mi.gov
├── processing/
│   ├── downloader.py        # Video download logic
│   └── transcriber.py       # Whisper transcription + transcribe_pending
├── state/
│   └── manager.py           # Pipeline state tracking per video
├── utils/
│   ├── logging.py           # Logging setup
│   ├── notifications.py     # Telegram notifications
│   └── lock.py              # File lock (fcntl)
├── data/
│   ├── videos/
│   └── transcripts/
└── logs/
```

## Pipeline States (state/manager.py)

Each video is tracked through: `discovered` -> `downloading` -> `downloaded` -> `transcribing` -> `transcribed`

Other terminal states: `skipped`, `failed`

## Code Updates

### 1. Virtual Environment Activation in run_transcriber.sh

The wrapper script had the venv activation line commented out, causing `ModuleNotFoundError`.

**Fix:** Uncommented `source venv/bin/activate` and added `"$@"` to forward CLI args.

### 2. SSL Certificate Chain Workaround for house.mi.gov

`house.mi.gov` serves an incomplete SSL certificate chain (missing intermediate CA). Python's `requests` fails with `SSLCertVerificationError`. Installing `certifi` did not help since the problem is server-side.

**Fix:** `verify=False` on all `requests` calls in `processing/downloader.py` and `utils/notifications.py`, with `urllib3.disable_warnings()`.

### 3. Modular Refactor

Extracted the monolithic `house_transcriber.py` into separate modules:
- **scrapers/house.py** — Selenium driver init + video link scraping
- **processing/downloader.py** — Video download with SSL workaround
- **processing/transcriber.py** — Whisper transcription + idempotent `transcribe_pending()`
- **state/manager.py** — `StateManager` class with per-video pipeline states, replaces flat execution log
- **utils/** — logging, notifications, file lock
- **main.py** — orchestrator wiring everything together

The old `execution_log.json` format (`processed_videos`) is auto-migrated to the new format (`videos` with `state` field) on first load.

### 4. Bulk Download Optimization

The download loop was calling `state_manager.set_state()` 3 times per video (discovered, downloading, downloaded), each writing `execution_log.json` to disk. For 50 videos that's 150 disk writes during what should be a fast bulk download.

**Fix:** Removed all `set_state()` calls from inside the download loop. Downloads now run with zero disk writes. A single batch loop after all downloads completes updates state for every file that exists on disk. This matches the speed of the original script.

### 5. Correct Video Download URL

The scraper collects player page URLs (`/VideoArchivePlayer?video=FILENAME.mp4`), not direct video URLs. Downloading from these URLs produces 3.6KB HTML files instead of actual videos.

**Fix:** `processing/downloader.py` constructs the real download URL: `https://www.house.mi.gov/ArchiveVideoFiles/{filename}` (found by inspecting the JWPlayer config in the player page HTML).
