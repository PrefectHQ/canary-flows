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
from prefect.exceptions import ObjectNotFound
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
        default=10000,
        ge=1000,
        le=100000,
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
    logger.info(f"  cutoff: {cutoff.strftime('%Y-%m-%d %H:%M')} UTC")
    logger.info(f"  states: {', '.join(config.states_to_clean)}")

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
            logger.info("  no flow runs found matching criteria")
            return {"deleted": 0, "failed": 0, "total_found": 0}

        # Get total count for preview
        total_count = len(flow_runs)
        more = "+" if len(flow_runs) == config.batch_size else ""
        logger.info(f"  found: {total_count}{more} flow runs to clean")

        if config.dry_run:
            logger.info("  [DRY RUN] would delete these flow runs:")
            for fr in flow_runs[:5]:
                logger.info(f"    {fr.name} ({fr.state.name})")
            if len(flow_runs) > 5:
                logger.info(f"    ... and {len(flow_runs) - 5} more")
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
                    return flow_run_id, None, False
                except ObjectNotFound:
                    # Already deleted (404) - treat as success (idempotent)
                    return flow_run_id, None, True
                except Exception as e:
                    return flow_run_id, str(e), False

            results = await asyncio.gather(
                *[delete_with_error_handling(fr.id) for fr in flow_runs],
                return_exceptions=False,
            )

            # Process results
            already_deleted_count = 0
            for flow_run_id, error, was_already_deleted in results:
                if error:
                    failed_deletes.append((flow_run_id, error))
                    failed_total += 1
                else:
                    deleted_total += 1
                    if was_already_deleted:
                        already_deleted_count += 1

            batch_deleted = batch_size - len(failed_deletes)
            already_msg = f" ({already_deleted_count} already gone)" if already_deleted_count else ""
            logger.info(f"  batch: {batch_deleted}/{batch_size}{already_msg} | total: {deleted_total:,}")

            if failed_deletes:
                logger.warning(f"  {len(failed_deletes)} failures in batch:")
                for flow_run_id, error in failed_deletes[:3]:
                    logger.warning(f"    {flow_run_id}: {error}")

            # Rate limiting delay between batches
            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

            # Get next batch
            flow_runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter, limit=config.batch_size
            )

        logger.info(f"  done: {deleted_total:,} deleted, {failed_total:,} failed")
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
    try:
        secret = await Secret.load(secret_block_name)
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

        logger.info(f"  found: {event_count:,} events, {resource_count:,} event_resources older than {cutoff_days} days")

        if config.dry_run:
            logger.info("  [DRY RUN] no deletions performed")
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
            logger.info(f"  event_resources batch {batch_num}: {deleted:,} deleted | total: {total_resources_deleted:,}")

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
            logger.info(f"  events batch {batch_num}: {deleted:,} deleted | total: {total_events_deleted:,}")

            if deleted < batch_size:
                break

            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

        logger.info(f"  done: {total_events_deleted:,} events, {total_resources_deleted:,} event_resources")
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

    connection_string = await get_connection_string(config.db_secret_block)

    async with get_db_connection(connection_string) as conn:
        # Count orphans
        orphan_count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM event_resources er
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.id = er.event_id)
            """
        )

        logger.info(f"  found: {orphan_count:,} orphaned event_resources")

        if orphan_count == 0:
            return {"orphans_deleted": 0, "orphans_found": 0}

        if config.dry_run:
            logger.info("  [DRY RUN] no deletions performed")
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
            logger.info(f"  batch {batch_num}: {deleted:,} deleted | total: {total_deleted:,}")

            if deleted < batch_size:
                break

            if config.rate_limit_delay > 0:
                await asyncio.sleep(config.rate_limit_delay)

        logger.info(f"  done: {total_deleted:,} orphans deleted")
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

        logger.info(
            f"  events           {events_count:>12,}  ({events_older_than_retention:,} older than {config.days_to_keep} days)"
        )
        logger.info(
            f"  event_resources  {event_resources_count:>12,}  ({orphan_count:,} orphaned)"
        )

        return {
            "events_total": events_count,
            "events_older_than_retention": events_older_than_retention,
            "event_resources_total": event_resources_count,
            "event_resources_orphaned": orphan_count,
        }


def _section(title: str) -> str:
    """Format a section header."""
    return f"\n{'─' * 50}\n  {title}\n{'─' * 50}"


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

    # Header
    logger.info(_section("Database Cleanup"))
    mode = "DRY RUN" if config.dry_run else "LIVE"
    logger.info(f"  mode: {mode}  |  retention: {config.days_to_keep} days")
    logger.info(f"  targets: {', '.join(e.value for e in config.entity_types)}")

    stats = {"total_deleted": 0, "total_failed": 0}
    is_all = EntityType.ALL in config.entity_types

    # Get table stats first for visibility
    needs_sql = is_all or EntityType.EVENTS in config.entity_types or EntityType.ORPHANED_EVENT_RESOURCES in config.entity_types
    if needs_sql:
        logger.info(_section("Table Stats"))
        try:
            table_stats = await get_table_stats(config)
            stats["table_stats"] = table_stats
        except Exception as e:
            logger.warning(f"  Could not get table stats: {e}")

    # Clean up flow runs via API
    if EntityType.FLOW_RUNS in config.entity_types or is_all:
        logger.info(_section("Flow Runs (via API)"))
        result = await delete_old_flow_runs(config)
        stats["flow_runs_deleted"] = result["deleted"]
        stats["flow_runs_failed"] = result["failed"]
        stats["total_deleted"] += result["deleted"]
        stats["total_failed"] += result["failed"]

        if "dry_run" in result:
            stats["dry_run"] = True

    # Clean up events via direct SQL
    if EntityType.EVENTS in config.entity_types or is_all:
        logger.info(_section("Events (via SQL)"))
        try:
            result = await delete_old_events(config)
            stats["events_deleted"] = result.get("events_deleted", 0)
            stats["event_resources_deleted"] = result.get("event_resources_deleted", 0)
            stats["total_deleted"] += result.get("events_deleted", 0) + result.get("event_resources_deleted", 0)

            if "dry_run" in result:
                stats["dry_run"] = True
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats["events_error"] = str(e)

    # Clean up orphaned event_resources via direct SQL
    if EntityType.ORPHANED_EVENT_RESOURCES in config.entity_types or is_all:
        logger.info(_section("Orphaned Event Resources (via SQL)"))
        try:
            result = await delete_orphaned_event_resources(config)
            stats["orphans_deleted"] = result.get("orphans_deleted", 0)
            stats["total_deleted"] += result.get("orphans_deleted", 0)

            if "dry_run" in result:
                stats["dry_run"] = True
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats["orphans_error"] = str(e)

    # Summary
    logger.info(_section("Summary"))
    logger.info(f"  total deleted: {stats['total_deleted']:,}")
    if stats["total_failed"] > 0:
        logger.info(f"  total failed:  {stats['total_failed']:,}")
    logger.info("")

    return stats


if __name__ == "__main__":
    # Quick local test with dry run
    asyncio.run(database_cleanup_entry())
