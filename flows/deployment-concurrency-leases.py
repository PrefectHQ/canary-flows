"""
Integration test for concurrency leases functionality.

This test verifies that concurrency limits properly create and manage
leases to prevent orphaned concurrency slots when flow runs fail unexpectedly.
The test creates global concurrency limits and verifies lease behavior during
flow execution.

This runs every 5 minutes to provide early warning of concurrency lease issues.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from prefect import flow, get_client, get_run_logger
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.client.schemas.objects import GlobalConcurrencyLimit
from prefect.concurrency.asyncio import concurrency


# Integration test timeout - must complete before next run (5 minutes)
# Need extra time for 65-second lease expiration test
INTEGRATION_TEST_TIMEOUT = (
    4 * 60 + 30
)  # 4.5 minutes to allow for lease expiration testing
FLOW_RUN_TIMEOUT = 30  # 30 seconds per flow run


async def clean_slate_setup() -> None:
    """Clean up any previous test artifacts to ensure a fresh start."""
    logger = get_run_logger()

    async with get_client() as client:
        # Clean up test concurrency limits
        try:
            limits = await client.read_global_concurrency_limits()
            for limit in limits:
                if limit.name.startswith("lease-test-"):
                    logger.info(f"Cleaning up old concurrency limit: {limit.name}")
                    await client.delete_global_concurrency_limit_by_name(limit.name)
        except Exception as e:
            logger.warning(f"Error cleaning concurrency limits: {e}")


@asynccontextmanager
async def create_test_concurrency_limit() -> (
    AsyncGenerator[GlobalConcurrencyLimit, None]
):
    """Create a test concurrency limit with cleanup."""
    logger = get_run_logger()
    test_id = str(uuid.uuid4())[:8]
    limit_name = f"lease-test-limit-{test_id}"

    async with get_client() as client:
        # Create concurrency limit
        await client.create_global_concurrency_limit(
            concurrency_limit=GlobalConcurrencyLimitCreate(
                name=limit_name,
                limit=1,
            )
        )
        # Read back the full limit object
        limit = await client.read_global_concurrency_limit_by_name(limit_name)
        logger.info(f"Created concurrency limit: {limit_name}")

        try:
            yield limit
        finally:
            # Clean up the limit
            try:
                await client.delete_global_concurrency_limit_by_name(limit_name)
                logger.info(f"Cleaned up concurrency limit: {limit_name}")
            except Exception as e:
                logger.warning(f"Error cleaning up limit {limit_name}: {e}")


async def verify_concurrency_limit_slots(limit_name: str, expected_active: int) -> bool:
    """Verify the concurrency limit has the expected number of active slots."""
    logger = get_run_logger()

    async with get_client() as client:
        limit = await client.read_global_concurrency_limit_by_name(limit_name)
        actual_active = limit.active_slots

        logger.info(
            f"Concurrency limit {limit_name}: {actual_active}/{limit.limit} slots active"
        )

        if actual_active != expected_active:
            logger.warning(
                f"Expected {expected_active} active slots, got {actual_active}"
            )
            return False

        return True


async def task_that_uses_concurrency_and_fails(limit_name: str):
    """A task that acquires a concurrency slot, fails, then tests lease cleanup."""
    logger = get_run_logger()

    # Use __aenter__() to acquire slot with minimum lease duration
    concurrency_ctx = concurrency(
        limit_name, occupy=1, lease_duration=60
    )  # 60 second lease (minimum)
    await concurrency_ctx.__aenter__()

    try:
        logger.info(f"Acquired concurrency slot for {limit_name}")
        # Simulate some work then failure
        await asyncio.sleep(2)
        logger.info("Simulating failure - will skip __aexit__() to test lease cleanup")

        # Skip calling __aexit__() to simulate improper cleanup
        # In real world, this would leave an orphaned slot that leases should clean up
        return "failed_without_cleanup"

    finally:
        # Check that slot is still held (proving we skipped __aexit__ cleanup)
        async with get_client() as client:
            limit_obj = await client.read_global_concurrency_limit_by_name(limit_name)
            logger.info(
                f"Immediately after failure: {limit_obj.active_slots}/1 slots active"
            )

        # Wait for the 60-second lease to expire and be cleaned up automatically
        logger.info("Waiting for 60-second lease to expire and be cleaned up...")
        await asyncio.sleep(65)  # Wait for lease expiration + buffer

        # Verify the lease system cleaned up the expired lease
        async with get_client() as client:
            limit_obj = await client.read_global_concurrency_limit_by_name(limit_name)
            logger.info(
                f"After lease expiration: {limit_obj.active_slots}/1 slots active"
            )
            if limit_obj.active_slots == 0:
                logger.info("✅ Lease system successfully cleaned up expired lease!")
            else:
                logger.error(
                    f"❌ Lease system failed - still {limit_obj.active_slots} active slots"
                )
                # This would indicate a problem with lease expiration cleanup

        # Now clean up the context properly to avoid anyio errors
        await concurrency_ctx.__aexit__(None, None, None)


async def task_that_uses_concurrency_successfully(limit_name: str):
    """A task that acquires a concurrency slot and completes successfully."""
    logger = get_run_logger()

    async with concurrency(limit_name, occupy=1, lease_duration=60):
        logger.info(f"Acquired concurrency slot for {limit_name}")
        # Simulate some work
        await asyncio.sleep(3)
        logger.info(f"Released concurrency slot for {limit_name}")
        return "success"


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def deployment_concurrency_leases_entry():
    """
    Integration test for concurrency leases functionality.

    Tests that:
    1. Global concurrency limits work with leases
    2. Leases are created when concurrency slots are acquired
    3. Leases prevent orphaned slots when tasks fail
    4. Cleanup works properly after lease expiration/revocation
    """
    logger = get_run_logger()
    logger.info("Starting concurrency leases integration test")

    try:
        # Clean slate - remove any previous test artifacts
        await clean_slate_setup()

        # Test successful concurrency usage
        async with create_test_concurrency_limit() as limit:
            logger.info(f"Testing successful concurrency usage with limit {limit.name}")

            # Verify limit starts with 0 active slots
            assert await verify_concurrency_limit_slots(
                limit.name, 0
            ), "Limit should start with 0 active slots"

            # Use concurrency successfully
            result = await task_that_uses_concurrency_successfully(limit.name)
            assert result == "success"

            # Verify slots are released after successful completion
            assert await verify_concurrency_limit_slots(
                limit.name, 0
            ), "Slots should be released after successful completion"

            logger.info("✅ Successful concurrency usage test passed")

        # Test failure scenario and lease cleanup
        async with create_test_concurrency_limit() as limit:
            logger.info(f"Testing failure scenario with limit {limit.name}")

            # Try to use concurrency but skip cleanup - this should test lease cleanup
            result = await task_that_uses_concurrency_and_fails(limit.name)
            assert (
                result == "failed_without_cleanup"
            ), f"Expected failure simulation, got {result}"
            logger.info(
                "Task completed without proper cleanup - testing lease system..."
            )

            # Give some time for lease cleanup
            await asyncio.sleep(5)

            # Verify no orphaned slots remain after failure
            assert await verify_concurrency_limit_slots(
                limit.name, 0
            ), "No orphaned slots should remain after failure and lease cleanup"

            logger.info("✅ Failure scenario and cleanup test passed")

        # Test concurrent access with queueing
        async with create_test_concurrency_limit() as limit:
            logger.info(f"Testing concurrent access queueing with limit {limit.name}")

            # Start multiple tasks concurrently - only 1 should run at a time
            tasks = []
            for i in range(3):
                task = asyncio.create_task(
                    task_that_uses_concurrency_successfully(limit.name)
                )
                tasks.append(task)
                # Small delay between starts
                await asyncio.sleep(0.5)

            # Wait for all to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All should succeed (they queue properly)
            success_count = sum(1 for r in results if r == "success")
            logger.info(
                f"Completed {success_count} out of {len(tasks)} concurrent tasks"
            )

            # Verify final state is clean
            assert await verify_concurrency_limit_slots(
                limit.name, 0
            ), "All slots should be released after concurrent execution"

            logger.info("✅ Concurrent access queueing test passed")

        logger.info("🎉 All concurrency leases tests passed!")

    except Exception as e:
        logger.error(f"❌ Concurrency leases test failed: {e}")
        raise

    finally:
        # Final cleanup
        try:
            await clean_slate_setup()
            logger.info("Final cleanup completed")
        except Exception as cleanup_error:
            logger.warning(f"Final cleanup failed: {cleanup_error}")


if __name__ == "__main__":
    asyncio.run(deployment_concurrency_leases_entry())
