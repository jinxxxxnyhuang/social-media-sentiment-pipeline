import json
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError


load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "social_posts")
FAILED_LOAD_DIR = Path(os.getenv("FAILED_LOAD_DIR", "data/failed_loads"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)



def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_HOST,
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )


def load_valid_records(valid_records_path: str) -> List[Dict[str, Any]]:
    with open(valid_records_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_bulk_actions(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    actions = []

    for record in records:
        actions.append({
            "_op_type": "index",
            "_index": ES_INDEX,
            "_id": record["record_id"],
            "_source": {
                **record,
                "loaded_at": datetime.now(timezone.utc).isoformat()
            }
        })

    return actions


def save_failed_records(batch_id: str, failed_items: List[Dict[str, Any]]) -> str:
    FAILED_LOAD_DIR.mkdir(parents=True, exist_ok=True)

    output_path = FAILED_LOAD_DIR / f"failed_loads_{batch_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(failed_items, f, ensure_ascii=False, indent=2)

    return str(output_path)


def bulk_load_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {
            "records_loaded": 0,
            "records_failed": 0,
            "status": "no_records",
            "failed_records_path": None
        }

    batch_id = records[0].get("batch_id", "unknown_batch")
    actions = build_bulk_actions(records)

    es = get_es_client()

    failed_items = []

    try:
        success_count, errors = bulk(
            es,
            actions,
            raise_on_error=False,
            stats_only=False,
        )

        failed_items = errors or []
        failed_count = len(failed_items)

        status = "completed" if failed_count == 0 else "completed_with_errors"

        failed_path = None
        if failed_items:
            failed_path = save_failed_records(batch_id, failed_items)

        summary = {
            "batch_id": batch_id,
            "target": "elasticsearch",
            "index": ES_INDEX,
            "records_attempted": len(records),
            "records_loaded": success_count,
            "records_failed": failed_count,
            "status": status,
            "failed_records_path": failed_path,
            "loaded_at": datetime.now(timezone.utc).isoformat()
        }

        logger.info("Elasticsearch load summary: %s", summary)
        return summary

    except BulkIndexError as e:
        failed_items = e.errors
        failed_path = save_failed_records(batch_id, failed_items)

        logger.error(
            "BulkIndexError while loading batch=%s failed_count=%s",
            batch_id,
            len(failed_items)
        )

        return {
            "batch_id": batch_id,
            "target": "elasticsearch",
            "index": ES_INDEX,
            "records_attempted": len(records),
            "records_loaded": 0,
            "records_failed": len(failed_items),
            "status": "failed",
            "failed_records_path": failed_path,
            "error_message": str(e),
            "loaded_at": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        logger.exception("Unexpected Elasticsearch load failure for batch=%s", batch_id)

        return {
            "batch_id": batch_id,
            "target": "elasticsearch",
            "index": ES_INDEX,
            "records_attempted": len(records),
            "records_loaded": 0,
            "records_failed": len(records),
            "status": "failed",
            "failed_records_path": None,
            "error_message": str(e),
            "loaded_at": datetime.now(timezone.utc).isoformat()
        }


def load_valid_file(valid_records_path: str) -> Dict[str, Any]:
    records = load_valid_records(valid_records_path)
    return bulk_load_records(records)


if __name__ == "__main__":
    valid_path = os.getenv("VALID_RECORDS_PATH")

    if not valid_path:
        raise ValueError("VALID_RECORDS_PATH is required")

    result = load_valid_file(valid_path)
    print(json.dumps(result, indent=2))