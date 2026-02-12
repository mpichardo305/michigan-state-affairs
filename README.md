# Michigan Senate and House Hearings Pipeline

Automated pipeline that scrapes, downloads, transcribes, and publishes Michigan House and Senate hearing videos.

A GitHub Actions workflow runs the full pipeline 3x daily on a cron schedule: 11 AM, 4 PM, and 9 PM CT. It can also be triggered manually via `workflow_dispatch`.

## How It Works

1. **Scrape** -- Selenium scrapers discover new hearing videos from [house.mi.gov](https://house.mi.gov/VideoArchive) and the [Senate video archive](https://cloud.castus.tv/vod/misenate/?page=ALL)
2. **Download** -- Videos are downloaded locally (House via direct HTTP, Senate via HLS/ffmpeg)
3. **Transcribe** -- OpenAI Whisper (`small` model) generates timestamped transcripts
4. **QC** -- Segments are scored for silence hallucination, low confidence, repetition, and high temperature
5. **Grammar** -- Punctuation restoration (HuggingFace) and grammar correction (LanguageTool)
6. **Format** -- Transcripts are converted to human-readable Markdown
7. **Upload** -- Videos and final transcripts are uploaded to S3; Telegram notifications are sent on completion

## Project Structure

```
main.py                  # Orchestrator — ties all stages together
config.json              # Runtime configuration (sources, thresholds, paths)
scrapers/
  house.py               # Selenium scraper for House video archive
  senate.py              # Selenium scraper for Senate video archive
processing/
  downloader.py           # House video downloader (HTTP)
  senate_downloader.py    # Senate video downloader (HLS/ffmpeg)
  transcriber.py          # Whisper transcription
  qc.py                   # Quality control scoring
  grammar.py              # Punctuation + grammar correction
  formatter.py            # JSON → Markdown formatting
  s3_uploader.py          # S3 upload (runs in background thread)
state/
  manager.py              # Tracks per-video state via execution_log.json
utils/
  logging.py              # Rotating file + console logger
  lock.py                 # File lock to prevent concurrent runs
  notifications.py        # Telegram notifications
.github/workflows/
  pipeline.yml            # GitHub Actions cron (3x daily) + manual trigger
```

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Requires `ffmpeg` on PATH and a `.env` file with AWS + Telegram credentials.

## Usage

```bash
python main.py --source all      # Run full pipeline (house + senate)
python main.py --source house    # House only
python main.py --source senate   # Senate only
```

## Opportunities to develop further
I built this first version of a production system in three days. It was part of iterative testing locally before creating the prod version. While it works, there's definitely room to make project more robust. Here is a list of items I would tackle if I had more time.

1. Replace whisper with faster-whisper which is CTranslate2-based, reportedly ~4x faster on CPU. 

Right now, The big video processed in 2/11/26's run was the Governor's Budget Presentation at 2.1 GB, that alone accounted for ~27 min of processing of the 1-hour run. A video like this is standard from sessions that are 2.5 hours long in both the house and senate. Looking at the log, I confirmed Whisper on CPU is the bottleneck and is the big reason why the run is slow. It took 23 mins to transcribe vs 2.5 min download that video.

3. Harden State Manager. state/manager.py is the single source of truth for the pipeline and it's a risk to the reliability of the entire project.

It's currently permissive and accepts any state transition, lets metadata overwrite reserved keys, and would crash fatally on a corrupted JSON.

Currently any state can transition to any other state. The goal would be to define allowed transitions so bugs in main.py get caught immediately instead of silently corrupting the log.

2. Implement pipeline parallelism which downloads next video while transcribing current one. 

This is useful on days where there are multiple videos uploaded and can help shorten the run.

4. Create unit tests to test the reliability of the state manager

Some ideas of what the unit tests would be like for the state manager: 
Basic roundtrip: set_state → get_state. To test if the Core read/write works.
Invalid transition raises ValueError so discovered → transcribed gets rejected for not being proper flow.
Corrupted state file is detected and recovered from a backup instead of crashing the pipeline.
Valid full sequence actually passes, which is the default expected behavior.

