"""
Integration test for V1 tag-based concurrency limits.

This test verifies that V1 tag-based concurrency limits properly control the number
of concurrent task runs based on task tags. Tests creation, enforcement, monitoring,
and cleanup of tag-based concurrency limits.

This runs every 5 minutes to provide early warning of concurrency limit issues.
"""

import asyncio
import inspect
import os
import time
import uuid
from datetime import datetime, timezone
from typing import List

from prefect import flow, get_client, get_run_logger, task

# Integration test timeout - must complete before next run (5 minutes)
INTEGRATION_TEST_TIMEOUT = 4 * 60  # 4 minutes
TEST_TAG_PREFIX = "test-concurrency-" + os.getenv("PREFECT_VERSION", "main")

# Only delete limits older than this to avoid race conditions with concurrent runs
CLEANUP_AGE_THRESHOLD_SECONDS = 5 * 60  # 5 minutes (same as schedule interval)


async def cleanup_lingering_test_limits():
    """Clean up any lingering test concurrency limits from prior runs.

    Only deletes limits older than CLEANUP_AGE_THRESHOLD_SECONDS to avoid
    race conditions where concurrent test runs delete each other's limits.
    """
    logger = get_run_logger()
    now = datetime.now(timezone.utc)

    async with get_client() as client:
        try:
            # Read all concurrency limits
            limits = await client.read_concurrency_limits(limit=100, offset=0)

            for limit in limits:
                if limit.tag.startswith(TEST_TAG_PREFIX):
                    # Only delete if older than threshold to avoid race conditions
                    if limit.created is not None:
                        age_seconds = (now - limit.created).total_seconds()
                        if age_seconds < CLEANUP_AGE_THRESHOLD_SECONDS:
                            logger.debug(
                                f"Skipping cleanup of {limit.tag} - only {age_seconds:.0f}s old"
                            )
                            continue

                    await client.delete_concurrency_limit_by_tag(limit.tag)
                    logger.info(f"✅ Pre-test cleanup deleted lingering limit: {limit.tag}")

        except Exception as e:
            logger.warning(f"Error cleaning up lingering concurrency limits: {e}")


@task
async def concurrency_limited_task(task_id: int, duration: int = 10, test_tag: str = ""):
    """A task that runs for a specified duration to test concurrency limits."""
    logger = get_run_logger()
    logger.info(f"Task {task_id} starting with tag '{test_tag}' - will run for {duration} seconds")

    start_time = time.time()
    await asyncio.sleep(duration)
    end_time = time.time()

    actual_duration = end_time - start_time
    logger.info(f"Task {task_id} completed after {actual_duration:.1f} seconds")

    return {"task_id": task_id, "duration": actual_duration, "expected": duration}


def get_test_tag() -> str:
    """Get the current test tag from flow context."""
    # This will be set in the flow context
    return f"{TEST_TAG_PREFIX}-{int(time.time())}"


async def monitor_concurrency_limit_slots(tag: str, duration_seconds: int = 60) -> List[int]:
    """Monitor the active slots count for a concurrency limit over time."""
    logger = get_run_logger()
    slot_counts = []
    start_time = time.time()

    async with get_client() as client:
        while time.time() - start_time < duration_seconds:
            try:
                limit = await client.read_concurrency_limit_by_tag(tag)
                active_count = len(limit.active_slots)
                slot_counts.append(active_count)
                logger.info(f"Concurrency limit '{tag}' has {active_count} active slots")

                # If no active slots, we can break early
                if active_count == 0 and len(slot_counts) > 5:  # Wait for some samples first
                    break

                await asyncio.sleep(2)  # Check every 2 seconds
            except Exception as e:
                logger.warning(f"Error reading concurrency limit: {e}")
                break

    return slot_counts


async def verify_concurrency_limit_enforcement(
    tag: str,
    expected_max_concurrent: int,
    task_count: int,
    task_duration: int
) -> bool:
    """Verify that the concurrency limit is properly enforced."""
    logger = get_run_logger()

    # Create a unique tag for this test run
    test_tag = f"{tag}-{uuid.uuid4().hex[:8]}"

    async with get_client() as client:
        try:
            # Create the concurrency limit
            await client.create_concurrency_limit(
                tag=test_tag,
                concurrency_limit=expected_max_concurrent
            )
            logger.info(f"Created concurrency limit for tag '{test_tag}' with limit {expected_max_concurrent}")

            # Start monitoring slots in background
            monitor_task = asyncio.create_task(
                monitor_concurrency_limit_slots(test_tag, duration_seconds=task_duration + 20)
            )

            # Submit multiple tasks concurrently
            logger.info(f"Submitting {task_count} tasks with tag '{test_tag}'")
            task_futures = []
            for i in range(task_count):
                future = concurrency_limited_task.with_options(tags=["foo", test_tag]).submit(i, task_duration, test_tag)
                if inspect.isawaitable(future):
                    future = await future
                task_futures.append(future)
                await asyncio.sleep(0.5)  # Small delay between submissions

            # Wait for all tasks to complete
            for future in task_futures:
                maybe_coro = future.wait()
                if inspect.isawaitable(maybe_coro):
                    await maybe_coro
            logger.info(f"All {len(task_futures)} tasks completed")

            # Get the slot monitoring results
            slot_counts = await monitor_task

            # Verify that we never exceeded the concurrency limit
            max_concurrent_observed = max(slot_counts) if slot_counts else 0
            logger.info(f"Maximum concurrent slots observed: {max_concurrent_observed}")
            logger.info(f"Expected maximum: {expected_max_concurrent}")

            # Check that we enforced the limit properly
            if max_concurrent_observed > expected_max_concurrent:
                logger.error(f"⚠️ Concurrency limit exceeded! Observed {max_concurrent_observed}, limit was {expected_max_concurrent}")
                return False

            # Check that we actually used the concurrency limit (not just ran sequentially)
            if max_concurrent_observed == 0:
                logger.error("⚠️ No concurrent slots observed - concurrency limit may not be working")
                return False

            # Verify all slots are released
            await asyncio.sleep(2)
            final_limit = await client.read_concurrency_limit_by_tag(test_tag)
            final_active_slots = len(final_limit.active_slots)

            if final_active_slots != 0:
                logger.error(f"⚠️ {final_active_slots} slots still active after test completion")
                return False

            logger.info("🎉 All concurrency slots properly released")
            return True

        finally:
            # Clean up the concurrency limit
            try:
                await client.delete_concurrency_limit_by_tag(test_tag)
                logger.info(f"🗑️ Deleted concurrency limit for tag '{test_tag}'")
            except Exception as e:
                logger.warning(f"Error deleting concurrency limit: {e}")


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def tag_based_concurrency_limits_entry(
    concurrency_limit: int = 2,
    task_count: int = 5,
    task_duration: int = 8,
):
    """
    Integration test for V1 tag-based concurrency limits.

    Tests that:
    1. Tag-based concurrency limits can be created via PrefectClient
    2. Tasks with matching tags respect the concurrency limit
    3. Active slot holders are properly tracked
    4. All slots are released when tasks complete
    5. Concurrency limits can be deleted for cleanup

    Args:
        concurrency_limit: Maximum number of concurrent tasks (default: 2)
        task_count: Number of tasks to submit (default: 5)
        task_duration: How long each task should run in seconds (default: 8)
    """
    logger = get_run_logger()
    logger.info("🚦 Starting tag-based concurrency limits integration test")
    logger.info(f"🐢 Concurrency limit: {concurrency_limit}")
    logger.info(f"🧮 Task count: {task_count}")
    logger.info(f"⏰ Task duration: {task_duration} seconds")

    # Clean up any lingering test limits
    await cleanup_lingering_test_limits()

    try:
        # Run the main test
        test_passed = await verify_concurrency_limit_enforcement(
            tag=TEST_TAG_PREFIX,
            expected_max_concurrent=concurrency_limit,
            task_count=task_count,
            task_duration=task_duration
        )

        if test_passed:
            logger.info("✅ Tag-based concurrency limits test PASSED!")
        else:
            logger.error("❌ Tag-based concurrency limits test FAILED!")
            raise AssertionError("Tag-based concurrency limits test failed")

    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(tag_based_concurrency_limits_entry())
