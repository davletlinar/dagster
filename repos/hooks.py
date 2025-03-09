from dagster import AssetExecutionContext
import requests
import os
from dotenv import load_dotenv

import requests
from typing import Optional

load_dotenv()

def send_telegram_message(
    message: str,
    bot_token: str = os.environ["TELEGRAM_BOT_TOKEN"],
    chat_id: str = os.environ["TELEGRAM_CHAT_ID"],
    parse_mode: Optional[str] = "Markdown"
) -> None:
    """
    Send a message using Telegram Bot API
    
    Args:
        message: The text message to send
        bot_token: Telegram bot token
        chat_id: Telegram chat ID to send the message to
        parse_mode: Optional. Can be 'HTML' or 'Markdown' for formatted messages
    """
    
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    
    if parse_mode:
        payload["parse_mode"] = parse_mode
        
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to send Telegram message: {str(e)}")
    
    return 1


def notify_telegram_on_failure(context: AssetExecutionContext):
    """Send notification to Telegram when an asset fails"""
    asset_key = context.asset_key
    error = context.get_op_execution_context().error
    
    message = (
        f"❌ <b>Asset Failed</b>\n"
        f"Asset: {asset_key}\n"
        f"Error: {error}\n"
        f"Time: {context.partition_time_window.start}"
    )
    
    try:
        send_telegram_message(message=message)
    except Exception as e:
        context.log.error(f"Failed to send Telegram notification: {e}")