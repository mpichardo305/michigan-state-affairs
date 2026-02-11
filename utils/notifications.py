import os
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def send_notification(message, config, logger):
    """Send notification via configured channels"""
    notifications = config.get('notifications', {})

    if notifications.get('telegram', {}).get('enabled'):
        _send_telegram(message, logger)


def _send_telegram(message, logger):
    """Send Telegram notification"""
    try:
        bot_token = os.environ['TELEGRAM_BOT_TOKEN']
        chat_id = os.environ['TELEGRAM_CHAT_ID']
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        requests.post(url, data=data, timeout=10, verify=False)
        logger.info("Telegram notification sent")
    except KeyError as e:
        logger.warning(f"Telegram env var missing: {e}")
    except Exception as e:
        logger.warning(f"Failed to send Telegram notification: {e}")
