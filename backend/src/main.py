from mharvester import harvest_public_timeline
from processor import process_raw_file
from validator import validate_processed_file
from loader import load_valid_file
from batch_logger import build_pipeline_summary, save_batch_log
from index_manager import ensure_social_posts_index, ensure_pipeline_batches_index
from batch_logger import build_pipeline_summary, save_batch_log, save_batch_log_to_es
from datetime import datetime

with open("cron_test.txt", "a") as f:
    f.write(f"ran at {datetime.now()}\n")

def main():
    # Step 1: Harvest
    harvester_summary = harvest_public_timeline()

    # Step 2: Process
    processor_summary = process_raw_file(
        harvester_summary["raw_output_path"]
    )

    # Step 3: Validate
    validator_summary = validate_processed_file(
        processor_summary["processed_output_path"]
    )

    # Step 4: Load
    ensure_social_posts_index()
    ensure_pipeline_batches_index()

    loader_summary = load_valid_file(
        validator_summary["valid_records_path"]
    )

    # Step 5: Pipeline Summary
    pipeline_summary = build_pipeline_summary(
        harvester_summary,
        processor_summary,
        validator_summary,
        loader_summary
    )

    save_batch_log(pipeline_summary)
    
    save_batch_log_to_es(pipeline_summary)
    
    print("Pipeline completed.")
    print(pipeline_summary)


if __name__ == "__main__":
    main()