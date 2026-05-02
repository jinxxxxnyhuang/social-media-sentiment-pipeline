import json
import os
import time
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mastodon import Mastodon, MastodonError


load_dotenv()

MASTODON_BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.au")
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
STATE_DIR = Path(os.getenv("STATE_DIR", "data/state"))

LIMIT = int(os.getenv("MASTODON_LIMIT", "40"))
MAX_PAGES = int(os.getenv("MASTODON_MAX_PAGES", "10"))
REMOTE = os.getenv("MASTODON_REMOTE", "true").lower() == "true"

STATE_FILE = STATE_DIR / "mastodon_state.json"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


class HarvestError(Exception):
    """Raised when Mastodon harvesting fails."""


def get_client() -> Mastodon:
    return Mastodon(
        access_token=MASTODON_ACCESS_TOKEN,
        api_base_url=MASTODON_BASE_URL,
        request_timeout=15
    )


def load_last_seen_id() -> Optional[str]:
    if not STATE_FILE.exists():
        return None

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    return state.get("last_seen_id")


def save_last_seen_id(last_seen_id: str) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_seen_id": str(last_seen_id),
                "updated_at": datetime.now(timezone.utc).isoformat()
            },
            f,
            indent=2
        )


def save_raw_batch(batch_id: str, records: List[Dict[str, Any]]) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    output_path = RAW_DATA_DIR / f"mastodon_public_{batch_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, ensure_ascii=False, indent=2)

    return output_path


def fetch_public_posts_once(
    client: Mastodon,
    since_id: Optional[str],
    max_id: Optional[str],
    limit: int
) -> List[Dict[str, Any]]:
    return client.timeline_public(
        since_id=since_id,
        max_id=max_id,
        limit=limit,
        remote=REMOTE
    )


def harvest_public_timeline(
    limit: int = LIMIT,
    max_pages: int = MAX_PAGES,
    max_retries: int = 3
) -> Dict[str, Any]:
    batch_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    client = get_client()
    last_seen_id = load_last_seen_id()

    all_posts: List[Dict[str, Any]] = []
    seen_ids = set()
    errors: List[str] = []

    max_id = None

    logger.info("Starting Mastodon public harvest batch=%s since_id=%s", batch_id, last_seen_id)

    for page in range(max_pages):
        for attempt in range(max_retries):
            try:
                posts = fetch_public_posts_once(
                    client=client,
                    since_id=last_seen_id,
                    max_id=max_id,
                    limit=limit
                )
                break

            except MastodonError as e:
                wait_seconds = 2 ** attempt
                logger.warning(
                    "Mastodon API error on page=%s attempt=%s: %s",
                    page + 1,
                    attempt + 1,
                    e
                )

                if attempt == max_retries - 1:
                    message = f"Failed to fetch page {page + 1}: {str(e)}"
                    errors.append(message)
                    posts = []

                time.sleep(wait_seconds)

        if not posts:
            break

        for post in posts:
            post_id = str(post.get("id"))

            if not post_id or post_id in seen_ids:
                continue

            seen_ids.add(post_id)

            post["_source_platform"] = "mastodon"
            post["_harvest_type"] = "public_timeline"
            post["_batch_id"] = batch_id
            post["_harvested_at"] = datetime.now(timezone.utc).isoformat()

            all_posts.append(post)

        # Mastodon public timeline is usually newest → oldest.
        # max_id lets us paginate older posts within this run.
        max_id = str(posts[-1].get("id"))

    if all_posts:
        newest_id = max(str(post.get("id")) for post in all_posts)
        save_last_seen_id(newest_id)

    output_path = save_raw_batch(batch_id, all_posts)

    end_time = datetime.now(timezone.utc)

    status = "completed" if not errors else "completed_with_errors"

    summary = {
        "batch_id": batch_id,
        "source": "mastodon",
        "harvest_type": "public_timeline",
        "records_extracted": len(all_posts),
        "status": status,
        "errors": errors,
        "raw_output_path": str(output_path),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "last_seen_id_before": last_seen_id,
        "last_seen_id_after": load_last_seen_id()
    }

    logger.info("Finished Mastodon harvest: %s", summary)

    return summary


if __name__ == "__main__":
    result = harvest_public_timeline()
    print(json.dumps(result, indent=2))