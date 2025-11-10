"""
Database cleanup flow for the OSS testbed.

This flow provides flexible cleanup capabilities for old flow runs, logs, and artifacts
using configurable retention policies. It uses Pydantic models to create a user-friendly
form in the Prefect UI with validation.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal

from prefect import flow, get_run_logger
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


async def delete_old_flow_runs(config: RetentionConfig) -> dict[str, int]:
    """Delete flow runs older than the configured retention period.

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

    async with get_client() as client:
        # Quick preview
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
            return {"deleted": 0, "failed": 0, "total_found": len(preview), "dry_run": True}

        # Process batches concurrently using asyncio.Semaphore
        semaphore = asyncio.Semaphore(config.max_concurrent_batches)
        deleted_total = 0
        failed_total = 0
        fetched_total = 0

        async def process_batch(batch_num: int) -> dict[str, int]:
            """Process a single batch with semaphore control."""
            async with semaphore:
                flow_runs = await client.read_flow_runs(
                    flow_run_filter=flow_run_filter, limit=config.batch_size
                )

                if not flow_runs:
                    logger.info(f"Batch {batch_num}: No flow runs found")
                    return {"deleted": 0, "failed": 0, "fetched": 0}

                fetched = len(flow_runs)
                logger.info(f"Batch {batch_num}: Fetched {fetched} flow runs")

                # Delete all in this batch concurrently
                async def delete_one(flow_run_id):
                    try:
                        await client.delete_flow_run(flow_run_id)
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to delete {flow_run_id}: {e}")
                        return False

                results = await asyncio.gather(
                    *[delete_one(fr.id) for fr in flow_runs]
                )

                deleted = sum(results)
                failed = len(results) - deleted

                logger.info(f"Batch {batch_num}: Deleted {deleted}/{fetched}, Failed: {failed}")
                return {"deleted": deleted, "failed": failed, "fetched": fetched}

        # Submit many batch tasks concurrently - semaphore will control actual concurrency
        # Submit enough tasks that some will return empty (we stop when we see empty results)
        max_batches = 1000  # conservative upper bound
        batch_tasks = [process_batch(i) for i in range(1, max_batches + 1)]

        # Process results as they complete
        for coro in asyncio.as_completed(batch_tasks):
            result = await coro
            deleted_total += result["deleted"]
            failed_total += result["failed"]
            fetched_total += result["fetched"]

            # Once we see an empty batch, we can stop waiting for more
            if result["fetched"] == 0:
                logger.info("Found empty batch, cleanup complete")
                break

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
