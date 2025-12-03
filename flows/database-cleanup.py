"""
Database cleanup flow for the OSS testbed.

This flow provides flexible cleanup capabilities for old flow runs, events, and
event resources using configurable retention policies. It uses Pydantic models
to create a user-friendly form in the Prefect UI with validation.

For flow runs: Uses the Prefect API for safe deletion with proper cascade handling.
For events/event_resources: Uses direct SQL for efficient batch deletion when the
built-in event persister trimmer can't keep up with large datasets.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import AsyncGenerator, Literal

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
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
    EVENTS = "events"
    ORPHANED_EVENT_RESOURCES = "orphaned_event_resources"
    ALL = "all"


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
            description="Which terminal states to include in cleanup (for flow runs)",
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

    sql_batch_size: int = Field(
        default=50000,
        ge=1000,
        le=200000,
        description="Batch size for SQL deletions (events/event_resources)",
        json_schema_extra={"position": 4},
    )

    rate_limit_delay: float = Field(
        default=0.5,
        ge=0.0,
        le=10.0,
        description="Delay in seconds between batches (0-10s)",
        json_schema_extra={"position": 5},
    )

    db_secret_block: str = Field(
        default="oss-testbed-db-connection-string",
        description="Name of the Secret block containing the database connection string",
        json_schema_extra={"position": 6},
    )

    dry_run: bool = Field(
        default=True,
        description="If true, only preview what would be deleted without actually deleting",
        json_schema_extra={"position": 7},
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
        # Create filter for old completed flow runs using state names
        flow_run_filter = FlowRunFilter(
            start_time=FlowRunFilterStartTime(before_=cutoff),
            state=FlowRunFilterState(
                name=FlowRunFilterStateName(any_=config.states_to_clean)
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

        # Actual deletion logic - use concurrent deletes with gather
        deleted_total = 0
        failed_total = 0

        while flow_runs:
            batch_size = len(flow_runs)
            failed_deletes = []

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
            for flow_run_id, error in results:
                if error:
                    failed_deletes.append((flow_run_id, error))
                    failed_total += 1
                else:
                    deleted_total += 1

            batch_deleted = batch_size - len(failed_deletes)
            logger.info(
                f"Batch complete: deleted {batch_deleted}/{batch_size} "
                f"(total: {deleted_total}, failed: {failed_total})"
            )

            if failed_deletes:
                logger.warning(f"Failed to delete {len(failed_deletes)} flow runs in this batch")
                for flow_run_id, error in failed_deletes[:3]:
                    logger.warning(f"  - {flow_run_id}: {error}")

            # Rate limiting delay between batches
            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter, limit=config.batch_size
            )

        logger.info(
            f"Cleanup complete. Deleted: {deleted_total}, Failed: {failed_total}"
        )
        return {"deleted": deleted_total, "failed": failed_total, "total_found": total_count}


@asynccontextmanager
async def get_db_connection(
    connection_string: str,
) -> AsyncGenerator["asyncpg.Connection", None]:
    """Get a database connection from a connection string."""
    import asyncpg

    # Convert SQLAlchemy connection string to asyncpg format if needed
    # postgresql+asyncpg://user:pass@host:port/db -> postgresql://user:pass@host:port/db
    if "+asyncpg" in connection_string:
        connection_string = connection_string.replace("+asyncpg", "")

    conn = await asyncpg.connect(connection_string)
    try:
        yield conn
    finally:
        await conn.close()


async def get_connection_string(secret_block_name: str) -> str:
    """Load database connection string from a Secret block."""
    logger = get_run_logger()

    try:
        secret = await Secret.load(secret_block_name)
        logger.info(f"Loaded database connection from Secret block: {secret_block_name}")
        return secret.get()
    except Exception as e:
        raise ValueError(
            f"Could not load database connection from Secret block '{secret_block_name}'. "
            f"Create this block with the PostgreSQL connection string. Error: {e}"
        )


@task
async def delete_old_events(config: RetentionConfig) -> dict[str, int]:
    """Delete events older than the configured retention period using direct SQL.

    This bypasses the API and connects directly to the database for efficient
    batch deletion when the built-in event persister can't keep up.
    """
    logger = get_run_logger()

    cutoff_days = config.days_to_keep
    batch_size = config.sql_batch_size

    logger.info(f"Cleaning events older than {cutoff_days} days")
    logger.info(f"Batch size: {batch_size}, Dry run: {config.dry_run}")

    connection_string = await get_connection_string(config.db_secret_block)

    async with get_db_connection(connection_string) as conn:
        # First, get counts for preview
        event_count = await conn.fetchval(
            f"SELECT COUNT(*) FROM events WHERE occurred < NOW() - INTERVAL '{cutoff_days} days'"
        )
        resource_count = await conn.fetchval(
            f"""
            SELECT COUNT(*) FROM event_resources er
            JOIN events e ON er.event_id = e.id
            WHERE e.occurred < NOW() - INTERVAL '{cutoff_days} days'
            """
        )

        logger.info(f"Found {event_count:,} events older than {cutoff_days} days")
        logger.info(f"Found {resource_count:,} associated event_resources")

        if config.dry_run:
            logger.info("DRY RUN MODE - No actual deletions will occur")
            return {
                "events_deleted": 0,
                "event_resources_deleted": 0,
                "events_found": event_count,
                "event_resources_found": resource_count,
                "dry_run": True,
            }

        # Delete event_resources first (they reference events)
        total_resources_deleted = 0
        batch_num = 0
        while True:
            batch_num += 1
            result = await conn.execute(
                f"""
                DELETE FROM event_resources
                WHERE id IN (
                    SELECT er.id FROM event_resources er
                    JOIN events e ON er.event_id = e.id
                    WHERE e.occurred < NOW() - INTERVAL '{cutoff_days} days'
                    LIMIT {batch_size}
                )
                """
            )
            deleted = int(result.split()[-1]) if result else 0
            total_resources_deleted += deleted
            logger.info(f"Batch {batch_num}: deleted {deleted} event_resources (total: {total_resources_deleted:,})")

            if deleted < batch_size:
                break

            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

        # Now delete events
        total_events_deleted = 0
        batch_num = 0
        while True:
            batch_num += 1
            result = await conn.execute(
                f"""
                DELETE FROM events
                WHERE id IN (
                    SELECT id FROM events
                    WHERE occurred < NOW() - INTERVAL '{cutoff_days} days'
                    LIMIT {batch_size}
                )
                """
            )
            deleted = int(result.split()[-1]) if result else 0
            total_events_deleted += deleted
            logger.info(f"Batch {batch_num}: deleted {deleted} events (total: {total_events_deleted:,})")

            if deleted < batch_size:
                break

            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

        logger.info(
            f"Events cleanup complete. Deleted {total_events_deleted:,} events "
            f"and {total_resources_deleted:,} event_resources"
        )
        return {
            "events_deleted": total_events_deleted,
            "event_resources_deleted": total_resources_deleted,
            "events_found": event_count,
            "event_resources_found": resource_count,
        }


@task
async def delete_orphaned_event_resources(config: RetentionConfig) -> dict[str, int]:
    """Delete event_resources that reference non-existent events (orphans).

    This can happen when events are deleted but their event_resources aren't
    properly cleaned up due to the event persister's trim logic using a
    time-based condition on event_resources.updated rather than checking
    if the parent event still exists.
    """
    logger = get_run_logger()
    batch_size = config.sql_batch_size

    logger.info("Cleaning orphaned event_resources (where parent event doesn't exist)")
    logger.info(f"Batch size: {batch_size}, Dry run: {config.dry_run}")

    connection_string = await get_connection_string(config.db_secret_block)

    async with get_db_connection(connection_string) as conn:
        # Count orphans
        orphan_count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM event_resources er
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = er.event_id)
            """
        )

        logger.info(f"Found {orphan_count:,} orphaned event_resources")

        if orphan_count == 0:
            logger.info("No orphaned event_resources to clean up")
            return {"orphans_deleted": 0, "orphans_found": 0}

        if config.dry_run:
            logger.info("DRY RUN MODE - No actual deletions will occur")
            # Show sample of orphans
            samples = await conn.fetch(
                """
                SELECT er.id, er.event_id FROM event_resources er
                WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = er.event_id)
                LIMIT 5
                """
            )
            for sample in samples:
                logger.info(f"  Sample orphan: resource_id={sample['id']}, missing_event_id={sample['event_id']}")
            return {"orphans_deleted": 0, "orphans_found": orphan_count, "dry_run": True}

        # Delete orphans in batches
        total_deleted = 0
        batch_num = 0
        while True:
            batch_num += 1
            result = await conn.execute(
                f"""
                DELETE FROM event_resources
                WHERE id IN (
                    SELECT er.id FROM event_resources er
                    WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = er.event_id)
                    LIMIT {batch_size}
                )
                """
            )
            deleted = int(result.split()[-1]) if result else 0
            total_deleted += deleted
            logger.info(f"Batch {batch_num}: deleted {deleted} orphans (total: {total_deleted:,})")

            if deleted < batch_size:
                break

            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

        logger.info(f"Orphan cleanup complete. Deleted {total_deleted:,} orphaned event_resources")
        return {"orphans_deleted": total_deleted, "orphans_found": orphan_count}


@task
async def get_table_stats(config: RetentionConfig) -> dict[str, int]:
    """Get current table sizes for visibility."""
    logger = get_run_logger()

    connection_string = await get_connection_string(config.db_secret_block)

    async with get_db_connection(connection_string) as conn:
        events_count = await conn.fetchval("SELECT COUNT(*) FROM events")
        event_resources_count = await conn.fetchval("SELECT COUNT(*) FROM event_resources")

        # Get counts by age
        events_older_than_retention = await conn.fetchval(
            f"SELECT COUNT(*) FROM events WHERE occurred < NOW() - INTERVAL '{config.days_to_keep} days'"
        )

        orphan_count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM event_resources er
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = er.event_id)
            """
        )

        logger.info(f"Current table stats:")
        logger.info(f"  events: {events_count:,} total ({events_older_than_retention:,} older than {config.days_to_keep} days)")
        logger.info(f"  event_resources: {event_resources_count:,} total ({orphan_count:,} orphaned)")

        return {
            "events_total": events_count,
            "events_older_than_retention": events_older_than_retention,
            "event_resources_total": event_resources_count,
            "event_resources_orphaned": orphan_count,
        }


@flow(name="database-cleanup")
async def database_cleanup_entry(config: RetentionConfig = RetentionConfig()) -> dict[str, int]:
    """Clean up old data from the testbed database.

    This flow provides configurable cleanup of:
    - Flow runs: via Prefect API with proper cascade handling
    - Events: via direct SQL for efficient batch deletion
    - Orphaned event_resources: via direct SQL to clean up resources
      whose parent events were deleted

    Args:
        config: Retention configuration controlling what and how to clean

    Returns:
        Statistics about the cleanup operation
    """
    logger = get_run_logger()
    logger.info("Starting database cleanup")
    logger.info(f"Configuration: days_to_keep={config.days_to_keep}, dry_run={config.dry_run}")
    logger.info(f"Entity types: {[e.value for e in config.entity_types]}")

    stats = {"total_deleted": 0, "total_failed": 0}
    is_all = EntityType.ALL in config.entity_types

    # Get table stats first for visibility
    needs_sql = is_all or EntityType.EVENTS in config.entity_types or EntityType.ORPHANED_EVENT_RESOURCES in config.entity_types
    if needs_sql:
        try:
            table_stats = await get_table_stats(config)
            stats["table_stats"] = table_stats
        except Exception as e:
            logger.warning(f"Could not get table stats (DB connection may not be configured): {e}")

    # Clean up flow runs via API
    if EntityType.FLOW_RUNS in config.entity_types or is_all:
        result = await delete_old_flow_runs(config)
        stats["flow_runs_deleted"] = result["deleted"]
        stats["flow_runs_failed"] = result["failed"]
        stats["total_deleted"] += result["deleted"]
        stats["total_failed"] += result["failed"]

        if "dry_run" in result:
            stats["dry_run"] = True

    # Clean up events via direct SQL
    if EntityType.EVENTS in config.entity_types or is_all:
        try:
            result = await delete_old_events(config)
            stats["events_deleted"] = result.get("events_deleted", 0)
            stats["event_resources_deleted"] = result.get("event_resources_deleted", 0)
            stats["total_deleted"] += result.get("events_deleted", 0) + result.get("event_resources_deleted", 0)

            if "dry_run" in result:
                stats["dry_run"] = True
        except Exception as e:
            logger.error(f"Failed to clean up events: {e}")
            stats["events_error"] = str(e)

    # Clean up orphaned event_resources via direct SQL
    if EntityType.ORPHANED_EVENT_RESOURCES in config.entity_types or is_all:
        try:
            result = await delete_orphaned_event_resources(config)
            stats["orphans_deleted"] = result.get("orphans_deleted", 0)
            stats["total_deleted"] += result.get("orphans_deleted", 0)

            if "dry_run" in result:
                stats["dry_run"] = True
        except Exception as e:
            logger.error(f"Failed to clean up orphaned event_resources: {e}")
            stats["orphans_error"] = str(e)

    logger.info(f"Database cleanup finished: {stats}")
    return stats


if __name__ == "__main__":
    # Quick local test with dry run
    asyncio.run(database_cleanup_entry())
