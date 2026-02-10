import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def send_notification(message, config, logger):
    """Send notification via configured channels"""
    notifications = config.get('notifications', {})

    if notifications.get('telegram', {}).get('enabled'):
        _send_telegram(message, notifications['telegram'], logger)


def _send_telegram(message, telegram_config, logger):
    """Send Telegram notification"""
    try:
        url = f"https://api.telegram.org/bot{telegram_config['bot_token']}/sendMessage"
        data = {
            'chat_id': telegram_config['chat_id'],
            'text': message,
            'parse_mode': 'HTML'
        }
        requests.post(url, data=data, timeout=10, verify=False)
        logger.info("Telegram notification sent")
    except Exception as e:
        logger.warning(f"Failed to send Telegram notification: {e}")
