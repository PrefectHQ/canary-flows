"""
Integration test for deployment-level concurrency limits.

This test verifies that deployment concurrency limits properly control the number
of concurrent flow runs and handle different collision strategies (ENQUEUE vs CANCEL_NEW).
Tests deployment-level concurrency behavior distinct from global concurrency limits.

This runs every 5 minutes to provide early warning of deployment concurrency issues.
"""

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List
from uuid import UUID

from prefect import flow, get_client, get_run_logger
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName
from prefect.client.schemas.objects import (
    ConcurrencyLimitStrategy,
    ConcurrencyOptions,
    StateType,
)

# Integration test timeout - must complete before next run (5 minutes)
INTEGRATION_TEST_TIMEOUT = 4 * 60  # 4 minutes
TEST_DEPLOYMENT_PREFIX = "test-deploy-concurrency-" + os.getenv("PREFECT_VERSION", "main")

# Only delete deployments older than this to avoid race conditions with concurrent runs
CLEANUP_AGE_THRESHOLD_SECONDS = 5 * 60  # 5 minutes (same as schedule interval)

async def sweep_up_lingering_test_deployments():
    """Sweep up lingering test deployments from prior runs.

    Only deletes deployments older than CLEANUP_AGE_THRESHOLD_SECONDS to avoid
    race conditions where concurrent test runs delete each other's deployments.
    """
    logger = get_run_logger()
    now = datetime.now(timezone.utc)

    async with get_client() as client:
        try:
            deployments = await client.read_deployments(
                deployment_filter=DeploymentFilter(
                    name=DeploymentFilterName(
                        like_=TEST_DEPLOYMENT_PREFIX
                    )
                ),
            )

            if not deployments:
                logger.info("🎉 Pre-test sweeper found 0 lingering test deployments from prior runs!")
                return

            for deployment in deployments:
                # Only delete if older than threshold to avoid race conditions
                if deployment.created is not None:
                    age_seconds = (now - deployment.created).total_seconds()
                    if age_seconds < CLEANUP_AGE_THRESHOLD_SECONDS:
                        logger.debug(
                            f"Skipping cleanup of {deployment.name} - only {age_seconds:.0f}s old"
                        )
                        continue

                await client.delete_deployment(deployment.id)
                logger.info(f"🧹 Pre-test sweeper deleted test deployment: {deployment.name}")
        except Exception as e:
            logger.warning(f"Error sweeping up lingering test deployments: {e}")



async def monitor_flow_run_states(
    flow_run_ids: List[UUID], timeout_seconds: int = 120
) -> dict[UUID, StateType]:
    """Monitor flow run states until completion or timeout."""
    logger = get_run_logger()
    start_time = time.time()
    final_states: dict[UUID, StateType] = {}

    async with get_client() as client:
        while time.time() - start_time < timeout_seconds:
            all_completed = True

            for flow_run_id in flow_run_ids:
                if flow_run_id not in final_states:
                    flow_run = await client.read_flow_run(flow_run_id)

                    assert flow_run.state is not None
                    if flow_run.state.is_final():
                        final_states[flow_run_id] = flow_run.state.type
                        logger.info(f"Flow run {str(flow_run_id)[-8:]} completed with state: {flow_run.state.name}")
                    else:
                        all_completed = False
                        logger.info(f"Flow run {str(flow_run_id)[-8:]} current state: {flow_run.state.name}")

            if all_completed:
                break

            await asyncio.sleep(5)  # Check every 5 seconds

    # Get final states for any remaining runs
    async with get_client() as client:
        for flow_run_id in flow_run_ids:
            if flow_run_id not in final_states:
                flow_run = await client.read_flow_run(flow_run_id)
                assert flow_run.state is not None
                final_states[flow_run_id] = flow_run.state.type

    return final_states


async def verify_deployment_concurrency_slots_freed(deployment_id: UUID) -> bool:
    """Verify that all concurrency slots for a deployment have been freed."""
    logger = get_run_logger()
    
    async with get_client() as client:
        deployment = await client.read_deployment(deployment_id)
        
        if deployment.global_concurrency_limit is None:
            raise RuntimeError("No global concurrency limit found for deployment")
            
        active_slots = deployment.global_concurrency_limit.active_slots
        logger.info(f"Deployment {str(deployment_id)[-8:]} has {active_slots} active concurrency slots")
        
        return active_slots == 0
        


async def create_flow_runs_via_deployment(
    deployment_id: UUID,  duration: int, count: int = 4
) -> List[UUID]:
    """Create multiple flow runs for a deployment and return their IDs."""
    logger = get_run_logger()
    flow_run_ids: List[UUID] = []

    async with get_client() as client:
        # Create multiple flow runs
        for i in range(count):
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters={"flow_run_duration": duration},
                name=f"concurrency-test-{i+1}-{int(time.time())}"
            )
            flow_run_ids.append(flow_run.id)
            logger.info(f"Created flow run {i+1}: {flow_run.id}")

            # Small delay between creations to see ordering
            await asyncio.sleep(1)

    return flow_run_ids


@asynccontextmanager
async def create_test_deployment_context(
    deployment_name: str, concurrency_limit: int, collision_strategy: ConcurrencyLimitStrategy
):
    """Create a temporary test deployment with automatic cleanup."""
    logger = get_run_logger()

    async with get_client() as client:
        # First register our test flow
        test_flow = long_running_flow_for_concurrency_test
        flow_id = await client.create_flow(test_flow)

        # Create the deployment
        deployment_id = await client.create_deployment(
            name=deployment_name,
            flow_id=flow_id,
            entrypoint="flows/deployment-concurrency.py:long_running_flow_for_concurrency_test",
            concurrency_limit=concurrency_limit,
            concurrency_options=ConcurrencyOptions(
                collision_strategy=collision_strategy,
            ),
            work_pool_name="integration-tests",
            pull_steps=[
                {
                    "prefect.deployments.steps.git_clone": {
                        "repository": "https://github.com/PrefectHQ/canary-flows",
                        "branch": "main",
                    }
                }
            ],
        )
        logger.info(f"Created test deployment: {deployment_name} with limit {concurrency_limit}")

        # Yield the deployment ID - cleanup is handled by sweep_up_lingering_test_deployments()
        # which only deletes deployments older than CLEANUP_AGE_THRESHOLD_SECONDS.
        # This prevents race conditions where the deployment is deleted while
        # child flow run pods are still starting up.
        yield deployment_id


@flow
async def long_running_flow_for_concurrency_test(flow_run_duration: int = 30):
    """A flow that runs for a specified duration to test concurrency limits."""
    logger = get_run_logger()
    logger.info(f"Starting long-running flow, will run for {flow_run_duration} seconds")

    start_time = time.time()
    await asyncio.sleep(flow_run_duration)
    end_time = time.time()

    actual_duration = end_time - start_time
    logger.info(f"Flow completed after {actual_duration:.1f} seconds")

    return {"duration": actual_duration, "expected": flow_run_duration}


async def test_enqueue_collision_strategy(test_run_duration_sec: int):
    """Test ENQUEUE collision strategy - runs should queue when limit is reached."""
    logger = get_run_logger()
    logger.info("Testing ENQUEUE collision strategy...")

    # Create temporary deployment with ENQUEUE strategy
    test_id = str(uuid.uuid4())[:8]
    async with create_test_deployment_context(
        f"{TEST_DEPLOYMENT_PREFIX}-enqueue-{test_id}",
        concurrency_limit=2,
        collision_strategy=ConcurrencyLimitStrategy.ENQUEUE
    ) as deployment_id:
        # Create 4 flow runs (2 should run immediately, 2 should queue)
        flow_run_ids = await create_flow_runs_via_deployment(deployment_id, count=4, duration=test_run_duration_sec)

        # Monitor the flow runs
        final_states = await monitor_flow_run_states(flow_run_ids, timeout_seconds=120)

        # Analyze results
        completed_count = sum(1 for state in final_states.values() if state == StateType.COMPLETED)
        cancelled_count = sum(1 for state in final_states.values() if state == StateType.CANCELLED)

        logger.info(f"ENQUEUE test results: {completed_count} completed, {cancelled_count} cancelled")

        # With ENQUEUE strategy, all should eventually complete (none cancelled)
        assert completed_count == 4, f"Expected 4 completed runs, got {completed_count}"
        assert cancelled_count == 0, f"Expected 0 cancelled runs, got {cancelled_count}"

        # Wait a moment for concurrency slots to be released
        await asyncio.sleep(3)
        
        # Verify that all deployment concurrency slots have been freed
        assert await verify_deployment_concurrency_slots_freed(deployment_id), \
            "Deployment concurrency slots were not freed after test completion"

        logger.info("✅ ENQUEUE strategy test passed - all runs completed and slots freed")


async def test_cancel_new_collision_strategy(test_run_duration_sec: int):
    """Test CANCEL_NEW collision strategy - new runs should be cancelled when limit is reached."""
    logger = get_run_logger()
    logger.info("Testing CANCEL_NEW collision strategy...")

    # Create temporary deployment with CANCEL_NEW strategy
    test_id = str(uuid.uuid4())[:8]
    async with create_test_deployment_context(
        f"{TEST_DEPLOYMENT_PREFIX}-cancel-new-{test_id}",
        concurrency_limit=2,
        collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
    ) as deployment_id:
        # Create 4 flow runs (2 should run, 2 should be cancelled)
        flow_run_ids = await create_flow_runs_via_deployment(deployment_id, count=4, duration=test_run_duration_sec)

        # Monitor the flow runs
        final_states = await monitor_flow_run_states(flow_run_ids, timeout_seconds=90)

        # Analyze results
        completed_count = sum(1 for state in final_states.values() if state == StateType.COMPLETED)
        cancelled_count = sum(1 for state in final_states.values() if state == StateType.CANCELLED)

        logger.info(f"CANCEL_NEW test results: {completed_count} completed, {cancelled_count} cancelled")

        # With CANCEL_NEW strategy, expect 2 completed and 2 cancelled
        assert completed_count == 2, f"Expected 2 completed runs, got {completed_count}"
        assert cancelled_count == 2, f"Expected 2 cancelled runs, got {cancelled_count}"

        # Wait a moment for concurrency slots to be released
        await asyncio.sleep(3)
        
        # Verify that all deployment concurrency slots have been freed
        assert await verify_deployment_concurrency_slots_freed(deployment_id), \
            "Deployment concurrency slots were not freed after test completion"

        logger.info("✅ CANCEL_NEW strategy test passed - all slots freed")


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def deployment_concurrency_entry(test_flow_duration_sec: int = 10):
    """
    Integration test for deployment-level concurrency limits.

    Tests that:
    1. Deployment concurrency limits properly control concurrent flow runs
    2. ENQUEUE collision strategy queues excess runs until slots are available
    3. CANCEL_NEW collision strategy cancels excess runs when limit is reached
    4. Concurrency limits work independently for different deployments
    """
    logger = get_run_logger()
    logger.info("🧪 Starting deployment concurrency integration test")
    logger.info(f"🕒 Test flow duration: {test_flow_duration_sec} seconds")

    # Sweep up any lingering test deployments from prior runs
    await sweep_up_lingering_test_deployments()

    try:
        # Test ENQUEUE collision strategy
        await test_enqueue_collision_strategy(test_flow_duration_sec)

        # Test CANCEL_NEW collision strategy
        await test_cancel_new_collision_strategy(test_flow_duration_sec)

        logger.info("🎉 All deployment concurrency tests passed!")

    except Exception as e:
        logger.error(f"❌ Deployment concurrency test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(deployment_concurrency_entry())
