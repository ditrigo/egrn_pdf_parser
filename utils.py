import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from lxml import etree


def serialize_datetime(obj: Any) -> Any:
    """
    Рекурсивно сериализует объекты datetime в строки.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [serialize_datetime(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize_datetime(value) for key, value in obj.items()}
    else:
        return obj


def parse_iso_date(date_str: str, logger: logging.Logger, field_name: str) -> Optional[datetime]:
    """
    Парсит строку в формате ISO в объект datetime.
    """
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        logger.warning(f"Некорректный формат {field_name}: {date_str}")
        return None


def extract_text(element: etree._Element, xpath: str) -> str:
    """
    Извлекает текст из XML-элемента по XPath.
    """
    text = element.findtext(xpath)
    return text.strip() if text else ""
