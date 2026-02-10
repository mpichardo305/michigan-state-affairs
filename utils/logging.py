import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(config):
    """Setup logging with file rotation and return configured logger"""
    log_config = config['logging']
    log_file = Path(log_config['file'])
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger('HouseTranscriber')
    logger.setLevel(log_config['level'])

    # Avoid adding duplicate handlers on re-init
    if logger.handlers:
        return logger

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=log_config['max_bytes'],
        backupCount=log_config['backup_count']
    )

    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
