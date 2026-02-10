#!/usr/bin/env python3
"""
Michigan House Hearing Video Transcriber
Automated video discovery, download, and transcription system
"""

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import time
import argparse
import sys
import fcntl
import whisper

class HouseTranscriber:
    """Main orchestrator for Michigan House hearing video processing"""
    
    def __init__(self, config_path="config.json", test_mode=False, force=False):
        """
        Initialize the transcriber
        
        Args:
            config_path: Path to JSON config file
            test_mode: If True, don't update execution log
            force: If True, reprocess all videos regardless of log
        """
        self.config = self._load_config(config_path)
        self.test_mode = test_mode
        self.force = force
        
        # Setup logging
        self._setup_logging()
        
        # Load execution log
        self.execution_log_path = Path(self.config['execution_log'])
        self.execution_log = self._load_execution_log()
        
        # Create directories
        self.video_dir = Path(self.config['download']['output_dir'])
        self.transcript_dir = Path(self.config['transcription']['output_dir'])
        self.video_dir.mkdir(parents=True, exist_ok=True)
        self.transcript_dir.mkdir(parents=True, exist_ok=True)
        
        # Lock file for preventing concurrent runs
        self.lock_file = Path(self.config['lock_file'])
        self.lock_fd = None
        
        # Selenium driver (initialized on demand)
        self.driver = None
        
        # Stats for summary
        self.stats = {
            'discovered': 0,
            'downloaded': 0,
            'transcribed': 0,
            'skipped': 0,
            'failed': 0
        }
        
        self.logger.info("=" * 70)
        self.logger.info("Michigan House Transcriber initialized")
        if self.test_mode:
            self.logger.info("TEST MODE: Execution log will NOT be updated")
        if self.force:
            self.logger.info("FORCE MODE: Will reprocess all videos")
        self.logger.info("=" * 70)
    
    def _load_config(self, config_path):
        """Load configuration from JSON file"""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def _setup_logging(self):
        """Setup logging with file rotation"""
        log_config = self.config['logging']
        log_file = Path(log_config['file'])
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('HouseTranscriber')
        self.logger.setLevel(log_config['level'])
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config['max_bytes'],
            backupCount=log_config['backup_count']
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _load_execution_log(self):
        """Load execution log or create new one"""
        if self.execution_log_path.exists():
            with open(self.execution_log_path, 'r') as f:
                data = json.load(f)
            data.setdefault('processed_videos', {})
            return data
        return {'processed_videos': {}}
    
    def _save_execution_log(self):
        """Save execution log atomically"""
        if self.test_mode:
            self.logger.info("TEST MODE: Skipping execution log update")
            return
        
        temp_file = self.execution_log_path.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.execution_log, f, indent=2)
        temp_file.replace(self.execution_log_path)
    
    def acquire_lock(self, timeout=60):
        """Acquire file lock to prevent concurrent runs"""
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_fd = open(self.lock_file, 'w')
        
        start_time = time.time()
        while True:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.lock_fd.write(str(datetime.now().isoformat()))
                self.lock_fd.flush()
                return True
            except IOError:
                if time.time() - start_time > timeout:
                    self.logger.error("Could not acquire lock - another instance running?")
                    return False
                time.sleep(1)
    
    def release_lock(self):
        """Release file lock"""
        if self.lock_fd:
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
            self.lock_fd.close()
            self.lock_fd = None
            try:
                self.lock_file.unlink()
            except FileNotFoundError:
                pass
    
    def _init_driver(self):
        """Initialize Selenium WebDriver with anti-detection settings"""
        if self.driver:
            return
        
        self.logger.info("Initializing Chrome WebDriver...")
        
        chrome_options = Options()
        
        # Headless mode
        if self.config.get('selenium', {}).get('headless', True):
            chrome_options.add_argument('--headless')
        
        # Anti-detection flags (similar to heatperks)
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Additional flags
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # User agent
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Remove webdriver property
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        
        self.logger.info("Chrome WebDriver initialized")
    
    def _quit_driver(self):
        """Quit the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.logger.info("Chrome WebDriver closed")
    
    def scrape_videos(self):
        """Scrape Michigan House video archive using Selenium"""
        self.logger.info(f"Scraping: {self.config['sources']['house']['url']}")
        
        self._init_driver()
        
        try:
            # Navigate to archive page
            self.driver.get(self.config['sources']['house']['url'])
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Give page extra time to render
            time.sleep(2)
            
            # Find all video links
            video_links = self.driver.find_elements(
                By.XPATH,
                "//a[contains(@href, '/VideoArchivePlayer?video=')]"
            )
            
            self.logger.info(f"Found {len(video_links)} video links")
            
            videos = []
            
            for link in video_links:
                try:
                    href = link.get_attribute('href')
                    
                    if not href or '/VideoArchivePlayer?video=' not in href:
                        continue
                    
                    # Extract filename
                    filename = href.split('video=')[1]
                    
                    # Get parent container for metadata
                    parent = link.find_element(By.XPATH, "./ancestor::li[contains(@class, 'page-search-container')]")
                    
                    # Extract category (from strong tag)
                    try:
                        category_elem = parent.find_element(By.TAG_NAME, 'strong')
                        category = category_elem.text.strip()
                    except:
                        category = "Unknown"
                    
                    # Extract date (text near the link)
                    try:
                        # The date is usually in a nearby text node
                        parent_text = parent.text
                        # Split by newlines and find the date-like string
                        lines = [line.strip() for line in parent_text.split('\n') if line.strip()]
                        date = lines[-1] if lines else "Unknown"
                    except:
                        date = "Unknown"
                    
                    videos.append({
                        'filename': filename,
                        'category': category,
                        'date': date,
                        'url': href
                    })
                    
                except Exception as e:
                    self.logger.warning(f"Failed to parse video link: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(videos)} videos")
            return videos
            
        except Exception as e:
            self.logger.error(f"Failed to scrape videos: {e}")
            return []
    
    def is_processed(self, filename):
        """Check if video has been processed"""
        if self.force:
            return False
        return filename in self.execution_log['processed_videos']
    
    def mark_processed(self, filename, metadata):
        """Mark video as processed in execution log"""
        self.execution_log['processed_videos'][filename] = {
            'processed_at': datetime.now().isoformat(),
            'category': metadata.get('category'),
            'date': metadata.get('date'),
            'local_path': metadata.get('local_path'),
            'transcript_path': metadata.get('transcript_path')
        }
    
    def download_video(self, video):
        """Download a video file"""
        filename = video['filename']
        output_path = self.video_dir / filename
        
        # Check if already downloaded
        if output_path.exists():
            self.logger.info(f"Video already exists: {filename}")
            return output_path
        
        self.logger.info(f"Downloading: {filename}")
        
        try:
            response = requests.get(
                video['url'],
                stream=True,
                timeout=self.config['download']['timeout'],
                verify=False
            )
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            chunk_size = self.config['download']['chunk_size']
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log progress every 10MB
                        if downloaded % (10 * 1024 * 1024) < chunk_size:
                            if total_size > 0:
                                progress = (downloaded / total_size) * 100
                                self.logger.info(f"  Progress: {progress:.1f}%")
            
            self.logger.info(f"Downloaded: {filename} ({downloaded / 1024 / 1024:.1f} MB)")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Download failed for {filename}: {e}")
            # Clean up partial download
            if output_path.exists():
                output_path.unlink()
            raise
    
    def transcribe_video(self, video_path, filename):
        """Transcribe video using Whisper"""
        transcript_file = self.transcript_dir / f"{video_path.stem}.json"

        # Check if already transcribed
        if transcript_file.exists():
            self.logger.info(f"Transcript already exists: {filename}")
            return transcript_file

        self.logger.info(f"Transcribing: {filename}")

        try:
            # Load Whisper model
            model_name = self.config['transcription']['model']
            self.logger.info(f"Loading Whisper model: {model_name}")
            model = whisper.load_model(model_name)

            # Transcribe
            self.logger.info("Running transcription (this may take a while)...")
            result = model.transcribe(str(video_path))

            # Save transcript
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

            self.logger.info(f"Transcription complete: {filename}")
            return transcript_file

        except Exception as e:
            self.logger.error(f"Transcription failed for {filename}: {e}")
            # Clean up partial transcript
            if transcript_file.exists():
                transcript_file.unlink()
            raise
    
    def transcribe_pending(self):
        """Transcribe any downloaded videos that are missing transcripts"""
        self.logger.info("Checking for videos pending transcription...")

        video_files = sorted(self.video_dir.glob("*.mp4"))
        pending = []

        for video_path in video_files:
            transcript_file = self.transcript_dir / f"{video_path.stem}.json"
            if not transcript_file.exists():
                pending.append(video_path)

        if not pending:
            self.logger.info("No videos pending transcription")
            return

        self.logger.info(f"Found {len(pending)} videos pending transcription")
        transcribed = 0
        failed = 0

        for i, video_path in enumerate(pending, 1):
            filename = video_path.name
            self.logger.info(f"[{i}/{len(pending)}] Transcribing: {filename}")
            try:
                transcript_path = self.transcribe_video(video_path, filename)
                self.stats['transcribed'] += 1
                transcribed += 1

                # Update execution log entry if it exists
                if filename in self.execution_log.get('processed_videos', {}):
                    self.execution_log['processed_videos'][filename]['transcript_path'] = str(transcript_path)
                    self._save_execution_log()

            except Exception as e:
                self.logger.error(f"Failed to transcribe {filename}: {e}")
                self.stats['failed'] += 1
                failed += 1

        self.logger.info(f"Transcription catch-up complete: {transcribed} transcribed, {failed} failed")

    def process_video(self, video):
        """Process a single video (download + transcribe)"""
        filename = video['filename']
        
        # Check if already processed
        if self.is_processed(filename):
            self.logger.info(f"Skipping (already processed): {filename}")
            self.stats['skipped'] += 1
            return
        
        try:
            # Download
            video_path = self.download_video(video)
            self.stats['downloaded'] += 1
            
            # Transcribe
            # transcript_path = self.transcribe_video(video_path, filename)
            # self.stats['transcribed'] += 1
            
            # Mark as processed
            self.mark_processed(filename, {
                'category': video['category'],
                'date': video['date'],
                'local_path': str(video_path),
                # 'transcript_path': str(transcript_path)
            })
            
            self.logger.info(f"✓ Completed: {filename}")
            
        except Exception as e:
            self.logger.error(f"✗ Failed to process {filename}: {e}")
            self.stats['failed'] += 1
    
    def send_notification(self, message):
        """Send notification via configured channels"""
        notifications = self.config.get('notifications', {})
        
        # Telegram
        if notifications.get('telegram', {}).get('enabled'):
            self._send_telegram(message, notifications['telegram'])
    
    def _send_telegram(self, message, config):
        """Send Telegram notification"""
        try:
            url = f"https://api.telegram.org/bot{config['bot_token']}/sendMessage"
            data = {
                'chat_id': config['chat_id'],
                'text': message,
                'parse_mode': 'HTML'
            }
            requests.post(url, data=data, timeout=10, verify=False)
            self.logger.info("Telegram notification sent")
        except Exception as e:
            self.logger.warning(f"Failed to send Telegram notification: {e}")
    
    def print_summary(self):
        """Print execution summary"""
        self.logger.info("=" * 70)
        self.logger.info("EXECUTION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Discovered:  {self.stats['discovered']}")
        self.logger.info(f"Downloaded:  {self.stats['downloaded']}")
        self.logger.info(f"Transcribed: {self.stats['transcribed']}")
        self.logger.info(f"Skipped:     {self.stats['skipped']}")
        self.logger.info(f"Failed:      {self.stats['failed']}")
        self.logger.info("=" * 70)
    
    def run(self):
        """Main execution flow"""
        # Acquire lock
        if not self.acquire_lock():
            return False
        
        try:
            # Scrape videos
            videos = self.scrape_videos()
            self.stats['discovered'] = len(videos)
            
            # Process each video
            for i, video in enumerate(videos, 1):
                self.logger.info(f"[{i}/{len(videos)}] Processing: {video['filename']}")
                self.process_video(video)

            # Transcribe any downloaded videos missing transcripts
            self.transcribe_pending()

            # Save execution log
            self._save_execution_log()
            
            # Print summary
            self.print_summary()
            
            # Send notification
            summary = (
                f"<b>Michigan House Transcriber</b>\n\n"
                f"✓ Discovered: {self.stats['discovered']}\n"
                f"✓ Downloaded: {self.stats['downloaded']}\n"
                f"✓ Transcribed: {self.stats['transcribed']}\n"
                f"⊘ Skipped: {self.stats['skipped']}\n"
                f"✗ Failed: {self.stats['failed']}"
            )
            self.send_notification(summary)
            
            return True
            
        except Exception as e:
            self.logger.exception(f"Fatal error: {e}")
            return False
        finally:
            # Close browser
            self._quit_driver()
            # Release lock
            self.release_lock()


def main():
    """Entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Michigan House Hearing Video Transcriber'
    )
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to config file (default: config.json)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - do not update execution log'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force reprocessing of all videos'
    )
    
    args = parser.parse_args()
    
    # Run transcriber
    transcriber = HouseTranscriber(
        config_path=args.config,
        test_mode=args.test,
        force=args.force
    )
    
    success = transcriber.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()