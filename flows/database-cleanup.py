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

from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
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

    states_to_clean: list[Literal["COMPLETED", "FAILED", "CANCELLED", "CRASHED"]] = (
        Field(
            default=["COMPLETED", "FAILED", "CANCELLED"],
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

    rate_limit_delay: float = Field(
        default=0.5,
        ge=0.0,
        le=10.0,
        description="Delay in seconds between API calls for rate limiting (0-10s)",
        json_schema_extra={"position": 4},
    )

    dry_run: bool = Field(
        default=True,
        description="If true, only preview what would be deleted without actually deleting",
        json_schema_extra={"position": 5},
    )


@task
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

    async with get_client() as client:
        # Convert state strings to StateType enum
        state_types = [StateType(state) for state in config.states_to_clean]

        # Create filter for old completed flow runs
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(any_=state_types)
            ),
        )

        # Get initial batch to count and preview
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter, limit=config.batch_size
        )

        if not flow_runs:
            logger.info("No flow runs found matching criteria")
            return {"deleted": 0, "failed": 0, "total_found": 0}

        # Get total count for preview
        total_count = len(flow_runs)
        if len(flow_runs) == config.batch_size:
            # There might be more, estimate
            logger.info(f"Found at least {total_count} flow runs to clean (may be more)")
        else:
            logger.info(f"Found {total_count} flow runs to clean")

        if config.dry_run:
            logger.info("DRY RUN MODE - No actual deletions will occur")
            logger.info(f"Would delete up to {total_count} flow runs")
            # Show sample of what would be deleted
            for i, fr in enumerate(flow_runs[:5]):
                logger.info(
                    f"  Sample {i+1}: {fr.name} ({fr.id}) "
                    f"from {fr.start_time}, state: {fr.state.name}"
                )
            if len(flow_runs) > 5:
                logger.info(f"  ... and {len(flow_runs) - 5} more in this batch")
            return {"deleted": 0, "failed": 0, "total_found": total_count, "dry_run": True}

        # Actual deletion logic
        deleted_total = 0
        failed_total = 0

        while flow_runs:
            batch_deleted = 0
            batch_failed = 0
            failed_deletes = []

            for flow_run in flow_runs:
                try:
                    await client.delete_flow_run(flow_run.id)
                    deleted_total += 1
                    batch_deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete flow run {flow_run.id}: {e}")
                    failed_deletes.append((flow_run.id, str(e)))
                    failed_total += 1
                    batch_failed += 1

                # Rate limiting
                if batch_deleted % 10 == 0 and config.rate_limit_delay > 0:
                    await asyncio.sleep(config.rate_limit_delay)

            logger.info(
                f"Batch complete: deleted {batch_deleted}/{len(flow_runs)} "
                f"(total: {deleted_total}, failed: {failed_total})"
            )

            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} flow runs in this batch")
                for flow_run_id, error in failed_deletes[:3]:
                    logger.warning(f"  - {flow_run_id}: {error}")

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter, limit=config.batch_size
            )

            # Delay between batches
            if flow_runs and config.rate_limit_delay > 0:
                await asyncio.sleep(1.0)

        logger.info(
            f"Cleanup complete. Deleted: {deleted_total}, Failed: {failed_total}"
        )
        return {"deleted": deleted_total, "failed": failed_total, "total_found": total_count}


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
