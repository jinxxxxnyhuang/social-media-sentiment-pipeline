import os
import logging
from typing import Dict, Any

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "social_posts")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


SOCIAL_POSTS_INDEX_CONFIG: Dict[str, Any] = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        "properties": {
            "record_id": {"type": "keyword"},
            "source": {"type": "keyword"},
            "source_post_id": {"type": "keyword"},
            "batch_id": {"type": "keyword"},

            "created_at": {"type": "date"},
            "harvested_at": {"type": "date"},
            "loaded_at": {"type": "date"},

            "url": {"type": "keyword"},
            "language": {"type": "keyword"},
            "topic": {"type": "keyword"},

            "content_clean": {"type": "text"},
            "hashtags": {"type": "keyword"},
            "instance": {"type": "keyword"},
            "sentiment_label": {"type": "keyword"},
            "sentiment_score": {"type": "float"},
            "sentiment_confidence": {"type": "float"},
            "media_only": {"type": "boolean"},

            "reblogs_count": {"type": "integer"},
            "favourites_count": {"type": "integer"},
            "replies_count": {"type": "integer"},

            "account_id": {"type": "keyword"},
            "account_username": {"type": "keyword"},
            "account_acct": {"type": "keyword"}
        }
    }
}

PIPELINE_BATCHES_INDEX = os.getenv("PIPELINE_BATCHES_INDEX", "pipeline_batches")

PIPELINE_BATCHES_INDEX_CONFIG = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        "properties": {
            "batch_id": {"type": "keyword"},
            "source": {"type": "keyword"},

            "records_extracted": {"type": "integer"},
            "records_processed": {"type": "integer"},
            "records_valid": {"type": "integer"},
            "records_invalid": {"type": "integer"},
            "records_loaded": {"type": "integer"},
            "records_failed_load": {"type": "integer"},
            "validation_error_count": {"type": "integer"},

            "harvester_status": {"type": "keyword"},
            "processor_status": {"type": "keyword"},
            "validator_status": {"type": "keyword"},
            "loader_status": {"type": "keyword"},
            "pipeline_status": {"type": "keyword"},

            "start_time": {"type": "date"},
            "end_time": {"type": "date"},
            "duration_seconds": {"type": "float"},
            "created_at": {"type": "date"}
        }
    }
}

def ensure_pipeline_batches_index() -> None:
    es = get_es_client()

    if es.indices.exists(index=PIPELINE_BATCHES_INDEX):
        logger.info("Elasticsearch index already exists: %s", PIPELINE_BATCHES_INDEX)
        return

    es.indices.create(
        index=PIPELINE_BATCHES_INDEX,
        body=PIPELINE_BATCHES_INDEX_CONFIG
    )

    logger.info("Created Elasticsearch index with mapping: %s", PIPELINE_BATCHES_INDEX)


def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_HOST,
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )


def ensure_social_posts_index() -> None:
    es = get_es_client()

    if es.indices.exists(index=ES_INDEX):
        logger.info("Elasticsearch index already exists: %s", ES_INDEX)
        return

    es.indices.create(
        index=ES_INDEX,
        body=SOCIAL_POSTS_INDEX_CONFIG
    )

    logger.info("Created Elasticsearch index with mapping: %s", ES_INDEX)





if __name__ == "__main__":
    ensure_social_posts_index()