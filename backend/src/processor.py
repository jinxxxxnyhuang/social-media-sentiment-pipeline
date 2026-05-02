import json
import os
import re
import html
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from transformers import pipeline

sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
    tokenizer="cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual"
)


load_dotenv()

PROCESSED_DATA_DIR = Path(os.getenv("PROCESSED_DATA_DIR", "data/processed"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)




def load_raw_posts(raw_file_path: str) -> List[Dict[str, Any]]:
    with open(raw_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_text(raw_html: Optional[str]) -> str:
    if not raw_html:
        return ""

    text = BeautifulSoup(raw_html, "html.parser").get_text(" ")
    text = html.unescape(text)
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalise_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None

    try:
        if isinstance(value, datetime):
            return value.isoformat()

        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).isoformat()

    except ValueError:
        return None



def multilingual_sentiment(text: str) -> Dict[str, Any]:
    if not text or len(text.strip()) < 10:
        return {
            "sentiment_label": None,
            "sentiment_confidence": None,
            "sentiment_score": None
        }

    try:
        result = sentiment_analyzer(text[:512])[0]
        label = result["label"].lower()
        confidence = round(float(result["score"]), 4)

        if label == "negative":
            directional_score = -confidence
        elif label == "positive":
            directional_score = confidence
        else:
            directional_score = 0.0

        return {
            "sentiment_label": label,
            "sentiment_confidence": confidence,
            "sentiment_score": directional_score
        }

    except Exception as e:
        logger.warning("Sentiment analysis failed: %s", str(e))
        return {
            "sentiment_label": None,
            "sentiment_confidence": None,
            "sentiment_score": None
        }


def extract_account_info(post: Dict[str, Any]) -> Dict[str, Optional[str]]:
    account = post.get("account") or {}

    return {
        "account_id": str(account.get("id")) if account.get("id") else None,
        "account_username": account.get("username"),
        "account_acct": account.get("acct"),
    }

def extract_hashtags(raw_html: Optional[str]) -> List[str]:
    if not raw_html:
        return []

    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text(" ")

    hashtags = re.findall(r"#([A-Za-z0-9_]+)", text)

    return list({tag.lower() for tag in hashtags})


def extract_instance(account_acct: Optional[str]) -> Optional[str]:
    if not account_acct:
        return None

    if "@" in account_acct:
        return account_acct.split("@")[-1]

    return None

def is_media_only(post: dict, content_clean: str) -> bool:
    media_attachments = post.get("media_attachments") or []
    return not content_clean.strip() and len(media_attachments) > 0

def is_trump_related(text: str) -> bool:
    text = text.lower()
    return "trump" in text or "maga" in text


def process_post(post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_html = post.get("content")
    content_clean = clean_text(raw_html)
    account_info = extract_account_info(post)

    hashtags = extract_hashtags(raw_html)
    instance = extract_instance(account_info.get("account_acct"))

    if not is_trump_related(content_clean):
        return None

    sentiment = multilingual_sentiment(content_clean)

    record_id = f"mastodon_{post.get('id')}" if post.get("id") else None

    
    processed = {
        "record_id": record_id,
        "source": "mastodon",
        "source_post_id": str(post.get("id")) if post.get("id") else None,
        "created_at": normalise_timestamp(post.get("created_at")),
        "harvested_at": post.get("_harvested_at"),
        "batch_id": post.get("_batch_id"),
        "url": post.get("url"),
        "language": post.get("language"),
        "content_clean": content_clean,
        "hashtags": hashtags,
        "instance": instance,
        "topic": "trump",
        "sentiment_label": sentiment["sentiment_label"],
        "sentiment_confidence": sentiment["sentiment_confidence"],
        "sentiment_score": sentiment["sentiment_score"],
        "reblogs_count": post.get("reblogs_count", 0),
        "favourites_count": post.get("favourites_count", 0),
        "replies_count": post.get("replies_count", 0),
        "media_only": is_media_only(post, content_clean),
        **account_info,
    }

    return processed


def process_raw_posts(raw_posts):
    processed_records = []

    for post in raw_posts:
        record = process_post(post)
        if record:
            processed_records.append(record)

    return processed_records


def save_processed_batch(batch_id: str, records: List[Dict[str, Any]]) -> Path:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PROCESSED_DATA_DIR / f"mastodon_processed_{batch_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return output_path


def process_raw_file(raw_file_path: str) -> Dict[str, Any]:
    raw_posts = load_raw_posts(raw_file_path)
    processed_records = process_raw_posts(raw_posts)

    batch_id = (
        processed_records[0].get("batch_id")
        if processed_records
        else "unknown_batch"
    )

    output_path = save_processed_batch(batch_id, processed_records)

    summary = {
        "batch_id": batch_id,
        "records_input": len(raw_posts),
        "records_processed": len(processed_records),
        "processed_output_path": str(output_path),
        "status": "completed"
    }

    logger.info("Processed batch summary: %s", summary)

    return summary


if __name__ == "__main__":
    raw_path = os.getenv("RAW_FILE_PATH")

    if not raw_path:
        raise ValueError("RAW_FILE_PATH is required")

    result = process_raw_file(raw_path)
    print(json.dumps(result, indent=2))