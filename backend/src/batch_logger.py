import json
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any
from elasticsearch import Elasticsearch

from dotenv import load_dotenv


load_dotenv()

BATCH_LOG_DIR = Path(os.getenv("BATCH_LOG_DIR", "data/batch_logs"))

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
PIPELINE_BATCHES_INDEX = os.getenv("PIPELINE_BATCHES_INDEX", "pipeline_batches")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


def build_pipeline_summary(
    harvester_summary: Dict[str, Any],
    processor_summary: Dict[str, Any],
    validator_summary: Dict[str, Any],
    loader_summary: Dict[str, Any]
) -> Dict[str, Any]:

    batch_id = harvester_summary.get("batch_id")

    total_duration_seconds = None
    try:
        start = datetime.fromisoformat(harvester_summary["start_time"])
        end = datetime.fromisoformat(loader_summary["loaded_at"])
        total_duration_seconds = (end - start).total_seconds()
    except Exception:
        pass

    status = "completed"

    for s in [
        harvester_summary.get("status"),
        processor_summary.get("status"),
        validator_summary.get("status"),
        loader_summary.get("status"),
    ]:
        if s in ["failed"]:
            status = "failed"
            break
        elif s in ["completed_with_errors"]:
            status = "completed_with_errors"

    summary = {
        "batch_id": batch_id,
        "source": harvester_summary.get("source"),

        "records_extracted": harvester_summary.get("records_extracted", 0),
        "records_processed": processor_summary.get("records_processed", 0),
        "records_valid": validator_summary.get("records_valid", 0),
        "records_invalid": validator_summary.get("records_invalid", 0),
        "records_loaded": loader_summary.get("records_loaded", 0),
        "records_failed_load": loader_summary.get("records_failed", 0),

        "validation_error_count": validator_summary.get("validation_error_count", 0),

        "harvester_status": harvester_summary.get("status"),
        "processor_status": processor_summary.get("status"),
        "validator_status": validator_summary.get("status"),
        "loader_status": loader_summary.get("status"),

        "pipeline_status": status,

        "start_time": harvester_summary.get("start_time"),
        "end_time": loader_summary.get("loaded_at"),
        "duration_seconds": total_duration_seconds,

        "created_at": datetime.now(timezone.utc).isoformat()
    }

    return summary


def save_batch_log(summary: Dict[str, Any]) -> str:
    BATCH_LOG_DIR.mkdir(parents=True, exist_ok=True)

    batch_id = summary.get("batch_id", "unknown_batch")

    output_path = BATCH_LOG_DIR / f"pipeline_batch_{batch_id}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info("Saved batch log: %s", output_path)

    return str(output_path)

def save_batch_log_to_es(summary: Dict[str, Any]) -> Dict[str, Any]:
    es = Elasticsearch(
        ES_HOST,
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )

    batch_id = summary.get("batch_id", "unknown_batch")

    response = es.index(
        index=PIPELINE_BATCHES_INDEX,
        id=batch_id,
        document=summary
    )

    logger.info("Saved batch log to Elasticsearch index=%s batch_id=%s", PIPELINE_BATCHES_INDEX, batch_id)

    return {
        "index": PIPELINE_BATCHES_INDEX,
        "batch_id": batch_id,
        "result": response.get("result")
    }