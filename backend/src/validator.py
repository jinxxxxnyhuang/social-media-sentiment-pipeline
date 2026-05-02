import json
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv


load_dotenv()

VALIDATION_DIR = Path(os.getenv("VALIDATION_DIR", "data/validation"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


ALLOWED_SOURCES = {"mastodon", "reddit"}
ALLOWED_SEVERITIES = {"low", "medium", "high"}


def load_processed_records(processed_file_path: str) -> List[Dict[str, Any]]:
    with open(processed_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_valid_timestamp(value: Any) -> bool:
    if not value:
        return False

    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def make_error(
    batch_id: str,
    record_id: str,
    field_name: str,
    issue_type: str,
    severity: str,
    message: str,
) -> Dict[str, Any]:
    return {
        "batch_id": batch_id,
        "record_id": record_id,
        "field_name": field_name,
        "issue_type": issue_type,
        "severity": severity,
        "message": message,
        "status": "open",
        "created_at": datetime.utcnow().isoformat()
    }


def validate_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    errors = []

    batch_id = str(record.get("batch_id") or "unknown_batch")
    record_id = str(record.get("record_id") or "unknown_record")

    if not record.get("record_id"):
        errors.append(make_error(
            batch_id, record_id, "record_id", "missing_record_id", "high",
            "Record ID is missing."
        ))

    if not record.get("source"):
        errors.append(make_error(
            batch_id, record_id, "source", "missing_source", "high",
            "Source is missing."
        ))
    elif record.get("source") not in ALLOWED_SOURCES:
        errors.append(make_error(
            batch_id, record_id, "source", "invalid_source", "high",
            f"Source must be one of {ALLOWED_SOURCES}."
        ))

    if not record.get("created_at"):
        errors.append(make_error(
            batch_id, record_id, "created_at", "missing_timestamp", "high",
            "Created timestamp is missing."
        ))
    elif not is_valid_timestamp(record.get("created_at")):
        errors.append(make_error(
            batch_id, record_id, "created_at", "invalid_timestamp", "high",
            "Created timestamp is not a valid ISO timestamp."
        ))

    if not record.get("content_clean"):
        errors.append(make_error(
            batch_id, record_id, "content_clean", "missing_content", "medium",
            "Cleaned content is missing or empty."
        ))

    if record.get("sentiment_score") is None:
        errors.append(make_error(
            batch_id, record_id, "sentiment_score", "missing_sentiment", "medium",
            "Sentiment score is missing."
        ))
    elif not is_number(record.get("sentiment_score")):
        errors.append(make_error(
            batch_id, record_id, "sentiment_score", "invalid_sentiment", "medium",
            "Sentiment score must be numeric."
        ))

    return errors


def validate_records(
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    valid_records = []
    invalid_records = []
    validation_errors = []

    seen_ids = set()

    for record in records:
        record_errors = validate_record(record)

        record_id = record.get("record_id")
        batch_id = str(record.get("batch_id") or "unknown_batch")

        if record_id:
            if record_id in seen_ids:
                record_errors.append(make_error(
                    batch_id,
                    str(record_id),
                    "record_id",
                    "duplicate_record_id",
                    "high",
                    "Duplicate record ID found within this batch."
                ))
            else:
                seen_ids.add(record_id)

        if record_errors:
            invalid_records.append(record)
            validation_errors.extend(record_errors)
        else:
            valid_records.append(record)

    return valid_records, invalid_records, validation_errors


def save_validation_outputs(
    batch_id: str,
    valid_records: List[Dict[str, Any]],
    invalid_records: List[Dict[str, Any]],
    validation_errors: List[Dict[str, Any]]
) -> Dict[str, str]:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    valid_path = VALIDATION_DIR / f"valid_records_{batch_id}.json"
    invalid_path = VALIDATION_DIR / f"invalid_records_{batch_id}.json"
    errors_path = VALIDATION_DIR / f"validation_errors_{batch_id}.json"

    with open(valid_path, "w", encoding="utf-8") as f:
        json.dump(valid_records, f, ensure_ascii=False, indent=2)

    with open(invalid_path, "w", encoding="utf-8") as f:
        json.dump(invalid_records, f, ensure_ascii=False, indent=2)

    with open(errors_path, "w", encoding="utf-8") as f:
        json.dump(validation_errors, f, ensure_ascii=False, indent=2)

    return {
        "valid_records_path": str(valid_path),
        "invalid_records_path": str(invalid_path),
        "validation_errors_path": str(errors_path),
    }


def validate_processed_file(processed_file_path: str) -> Dict[str, Any]:
    records = load_processed_records(processed_file_path)

    valid_records, invalid_records, validation_errors = validate_records(records)

    batch_id = (
        records[0].get("batch_id")
        if records and records[0].get("batch_id")
        else "unknown_batch"
    )

    output_paths = save_validation_outputs(
        batch_id=batch_id,
        valid_records=valid_records,
        invalid_records=invalid_records,
        validation_errors=validation_errors
    )

    status = "passed" if not validation_errors else "completed_with_errors"

    summary = {
        "batch_id": batch_id,
        "records_checked": len(records),
        "records_valid": len(valid_records),
        "records_invalid": len(invalid_records),
        "validation_error_count": len(validation_errors),
        "status": status,
        **output_paths
    }

    logger.info("Validation summary: %s", summary)

    return summary


if __name__ == "__main__":
    processed_path = os.getenv("PROCESSED_FILE_PATH")

    if not processed_path:
        raise ValueError("PROCESSED_FILE_PATH is required")

    result = validate_processed_file(processed_path)
    print(json.dumps(result, indent=2))