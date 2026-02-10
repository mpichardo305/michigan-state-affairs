import json
from datetime import datetime
from pathlib import Path

VALID_STATES = [
    'discovered',
    'downloading',
    'downloaded',
    'transcribing',
    'transcribed',
    'skipped',
    'failed',
]


class StateManager:
    """Tracks per-video pipeline state: discovered -> downloaded -> transcribed"""

    def __init__(self, log_path, test_mode=False):
        self.log_path = Path(log_path)
        self.test_mode = test_mode
        self.data = self._load()

    def _load(self):
        if self.log_path.exists():
            with open(self.log_path, 'r') as f:
                data = json.load(f)

            # Migrate old format (processed_videos) to new format (videos)
            if 'processed_videos' in data and 'videos' not in data:
                migrated = {}
                for filename, entry in data['processed_videos'].items():
                    has_transcript = entry.get('transcript_path') is not None
                    migrated[filename] = {
                        'state': 'transcribed' if has_transcript else 'downloaded',
                        'category': entry.get('category'),
                        'date': entry.get('date'),
                        'local_path': entry.get('local_path'),
                        'transcript_path': entry.get('transcript_path'),
                        'updated_at': entry.get('processed_at', datetime.now().isoformat()),
                    }
                return {'videos': migrated}

            data.setdefault('videos', {})
            return data

        return {'videos': {}}

    def save(self):
        if self.test_mode:
            return

        temp_file = self.log_path.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(self.data, f, indent=2)
        temp_file.replace(self.log_path)

    def get_state(self, filename):
        entry = self.data['videos'].get(filename)
        if entry is None:
            return None
        return entry.get('state')

    def set_state(self, filename, state, metadata=None):
        if state not in VALID_STATES:
            raise ValueError(f"Invalid state: {state}. Must be one of {VALID_STATES}")

        entry = self.data['videos'].setdefault(filename, {})
        entry['state'] = state
        entry['updated_at'] = datetime.now().isoformat()

        if metadata:
            entry.update(metadata)

        self.save()

    def get_videos_in_state(self, state):
        return [
            filename for filename, entry in self.data['videos'].items()
            if entry.get('state') == state
        ]

    def get_entry(self, filename):
        return self.data['videos'].get(filename)
