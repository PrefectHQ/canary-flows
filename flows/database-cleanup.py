"""
Database cleanup flow for the OSS testbed.

This flow provides flexible cleanup capabilities for old flow runs, logs, and artifacts
using configurable retention policies. It uses Pydantic models to create a user-friendly
form in the Prefect UI with validation.

The flow uses task-based parallelism to process multiple batches concurrently for
improved performance.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal

from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateName,
)
from pydantic import BaseModel, Field


class EntityType(str, Enum):
    """Types of entities that can be cleaned up."""

    FLOW_RUNS = "flow_runs"
    ALL = "all"  # Future: could expand to logs, artifacts, etc.


class RetentionConfig(BaseModel):
    """Configuration for data retention cleanup.

    This model creates a form in the Prefect UI with validation for all parameters.
    """

    days_to_keep: int = Field(
        default=30,
        ge=1,
        le=365,
        description="Number of days of data to retain (1-365 days)",
        json_schema_extra={"position": 0},
    )

    entity_types: list[EntityType] = Field(
        default=[EntityType.FLOW_RUNS],
        description="Types of entities to clean up",
        json_schema_extra={"position": 1},
    )

    states_to_clean: list[Literal["Completed", "Failed", "Cancelled", "Crashed"]] = (
        Field(
            default=["Completed", "Failed", "Cancelled"],
            description="Which terminal states to include in cleanup",
            json_schema_extra={"position": 2},
        )
    )

    batch_size: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Number of items to delete per batch (10-1000)",
        json_schema_extra={"position": 3},
    )

    max_concurrent_batches: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of batches to process concurrently (1-20)",
        json_schema_extra={"position": 4},
    )

    dry_run: bool = Field(
        default=True,
        description="If true, only preview what would be deleted without actually deleting",
        json_schema_extra={"position": 5},
    )


@task
async def delete_batch(
    flow_run_filter: FlowRunFilter,
    batch_size: int,
    batch_number: int,
    dry_run: bool = True,
) -> dict[str, int]:
    """Delete a single batch of flow runs.

    This task handles fetching and deleting one batch of flow runs concurrently.

    Args:
        flow_run_filter: Filter to select flow runs for deletion
        batch_size: Number of flow runs to fetch and delete
        batch_number: Batch number for logging
        dry_run: If True, skip actual deletion

    Returns:
        Statistics about this batch: deleted, failed, fetched counts
    """
    logger = get_run_logger()

    async with get_client() as client:
        # Fetch this batch
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter, limit=batch_size
        )

        if not flow_runs:
            logger.info(f"Batch {batch_number}: No flow runs found")
            return {"deleted": 0, "failed": 0, "fetched": 0}

        fetched_count = len(flow_runs)
        logger.info(f"Batch {batch_number}: Fetched {fetched_count} flow runs")

        if dry_run:
            logger.info(f"Batch {batch_number}: DRY RUN - skipping deletion")
            return {"deleted": 0, "failed": 0, "fetched": fetched_count, "dry_run": True}

        # Delete all flow runs in this batch concurrently
        async def delete_with_error_handling(flow_run_id):
            try:
                await client.delete_flow_run(flow_run_id)
                return flow_run_id, None
            except Exception as e:
                return flow_run_id, str(e)

        results = await asyncio.gather(
            *[delete_with_error_handling(fr.id) for fr in flow_runs],
            return_exceptions=False,
        )

        # Process results
        deleted_count = 0
        failed_count = 0
        failed_deletes = []

        for flow_run_id, error in results:
            if error:
                failed_deletes.append((flow_run_id, error))
                failed_count += 1
            else:
                deleted_count += 1

        logger.info(
            f"Batch {batch_number}: Deleted {deleted_count}/{fetched_count}, "
            f"Failed: {failed_count}"
        )

        if failed_deletes:
            logger.warning(
                f"Batch {batch_number}: Failed to delete {len(failed_deletes)} flow runs"
            )
            for flow_run_id, error in failed_deletes[:3]:
                logger.warning(f"  - {flow_run_id}: {error}")

        return {"deleted": deleted_count, "failed": failed_count, "fetched": fetched_count}


@task
async def delete_old_flow_runs(config: RetentionConfig) -> dict[str, int]:
    """Delete flow runs older than the configured retention period.

    This task coordinates multiple batch deletion tasks running concurrently.

    Args:
        config: Configuration for the cleanup operation

    Returns:
        Dictionary with statistics about the cleanup operation
    """
    logger = get_run_logger()

    cutoff = datetime.now(timezone.utc) - timedelta(days=config.days_to_keep)
    logger.info(f"Cleaning flow runs older than {cutoff} ({config.days_to_keep} days)")
    logger.info(f"States to clean: {config.states_to_clean}")
    logger.info(f"Dry run: {config.dry_run}")
    logger.info(f"Max concurrent batches: {config.max_concurrent_batches}")

    # Create filter for old flow runs
    flow_run_filter = FlowRunFilter(
        start_time=FlowRunFilterStartTime(before_=cutoff),
        state=FlowRunFilterState(
            name=FlowRunFilterStateName(any_=config.states_to_clean)
        ),
    )

    # Quick preview to see if there's anything to delete
    async with get_client() as client:
        preview = await client.read_flow_runs(
            flow_run_filter=flow_run_filter, limit=config.batch_size
        )

        if not preview:
            logger.info("No flow runs found matching criteria")
            return {"deleted": 0, "failed": 0, "total_found": 0}

        logger.info(f"Found at least {len(preview)} flow runs to process")

        if config.dry_run:
            logger.info("DRY RUN MODE - No actual deletions will occur")
            logger.info(f"Preview of first {min(5, len(preview))} flow runs:")
            for i, fr in enumerate(preview[:5]):
                logger.info(
                    f"  {i+1}. {fr.name} ({fr.id}) "
                    f"from {fr.start_time}, state: {fr.state.name}"
                )

    # Submit batch tasks concurrently
    # We'll keep submitting batches until we get an empty batch back
    batch_futures = []
    batch_number = 0
    active_batches = 0
    deleted_total = 0
    failed_total = 0
    fetched_total = 0

    while True:
        # Submit up to max_concurrent_batches at a time
        while active_batches < config.max_concurrent_batches:
            batch_number += 1
            future = delete_batch.submit(
                flow_run_filter=flow_run_filter,
                batch_size=config.batch_size,
                batch_number=batch_number,
                dry_run=config.dry_run,
            )
            batch_futures.append(future)
            active_batches += 1

        # Wait for at least one batch to complete
        if batch_futures:
            # Get result from the oldest batch
            completed_future = batch_futures.pop(0)
            result = await completed_future.wait()
            active_batches -= 1

            deleted_total += result["deleted"]
            failed_total += result["failed"]
            fetched_total += result["fetched"]

            # If this batch found no flow runs, we're done
            if result["fetched"] == 0:
                logger.info("No more flow runs to process, stopping batch submission")
                break
        else:
            # No more batches to submit
            break

    # Wait for any remaining batches to complete
    logger.info(f"Waiting for {len(batch_futures)} remaining batches to complete...")
    for future in batch_futures:
        result = await future.wait()
        deleted_total += result["deleted"]
        failed_total += result["failed"]
        fetched_total += result["fetched"]

    logger.info(
        f"Cleanup complete. Fetched: {fetched_total}, "
        f"Deleted: {deleted_total}, Failed: {failed_total}"
    )

    return_value = {"deleted": deleted_total, "failed": failed_total, "total_found": fetched_total}
    if config.dry_run:
        return_value["dry_run"] = True

    return return_value


@flow(name="database-cleanup")
async def database_cleanup_entry(config: RetentionConfig = RetentionConfig()) -> dict[str, int]:
    """Clean up old data from the testbed database.

    This flow provides configurable cleanup of old flow runs to manage database size.
    It uses the Prefect API for safe deletion with proper cascade handling.

    Args:
        config: Retention configuration controlling what and how to clean

    Returns:
        Statistics about the cleanup operation
    """
    logger = get_run_logger()
    logger.info("Starting database cleanup")
    logger.info(f"Configuration: days_to_keep={config.days_to_keep}, dry_run={config.dry_run}")

    stats = {"total_deleted": 0, "total_failed": 0}

    if EntityType.FLOW_RUNS in config.entity_types or EntityType.ALL in config.entity_types:
        result = await delete_old_flow_runs(config)
        stats["flow_runs_deleted"] = result["deleted"]
        stats["flow_runs_failed"] = result["failed"]
        stats["total_deleted"] += result["deleted"]
        stats["total_failed"] += result["failed"]

        if "dry_run" in result:
            stats["dry_run"] = True

    logger.info(f"Database cleanup finished: {stats}")
    return stats


if __name__ == "__main__":
    # Quick local test with dry run
    asyncio.run(database_cleanup_entry())
