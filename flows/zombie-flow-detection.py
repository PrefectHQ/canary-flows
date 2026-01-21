import asyncio
import os
import signal
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from uuid import uuid4
from prefect import flow, get_client, get_run_logger
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName
from prefect.events.clients import get_events_subscriber
from prefect.events.filters import (
    EventFilter,
    EventNameFilter,
    EventOccurredFilter,
    EventResourceFilter,
)
from prefect.exceptions import PrefectHTTPStatusError

# Test runs hourly, with 4 minute timeout (same as other integration tests)
INTEGRATION_TEST_TIMEOUT = 4 * 60

# Heartbeat configuration
HEARTBEAT_FREQUENCY = 30  # seconds
AUTOMATION_WINDOW = 90  # seconds - time without heartbeat before automation fires

# Subprocess script that runs serve() with heartbeats enabled
SERVE_SCRIPT = '''
import sys
import time
from prefect import flow, serve

@flow
def zombie_heartbeat_flow():
    """A flow that runs forever, emitting heartbeats until killed."""
    while True:
        time.sleep(10)

if __name__ == "__main__":
    deployment_name = sys.argv[1]
    serve(zombie_heartbeat_flow.to_deployment(name=deployment_name))
'''


@asynccontextmanager
async def create_or_replace_automation(
    automation: dict[str, Any],
) -> AsyncGenerator[dict[str, Any], None]:
    """Create an automation, cleaning up old ones with the same name prefix."""
    logger = get_run_logger()

    async with get_client() as prefect:
        # Clean up any older automations with the same name prefix
        response = await prefect._client.post("/automations/filter")
        response.raise_for_status()
        for existing in response.json():
            name = str(existing["name"])
            if name.startswith(automation["name"]):
                created = datetime.fromisoformat(
                    existing["created"].replace("Z", "+00:00")
                )
                age = datetime.now(timezone.utc) - created
                if age > timedelta(minutes=15):
                    logger.info(
                        "Deleting old automation %s (%s)",
                        existing["name"],
                        existing["id"],
                    )
                    try:
                        await prefect._client.delete(f"/automations/{existing['id']}")
                    except PrefectHTTPStatusError as e:
                        if e.response.status_code == 404:
                            logger.info("Automation %s already deleted", existing["id"])
                        else:
                            raise

        automation["name"] = f"{automation['name']}:{uuid4()}"

        response = await prefect._client.post("/automations", json=automation)
        response.raise_for_status()

        automation = response.json()
        logger.info("Created automation %s (%s)", automation["name"], automation["id"])

        logger.info(
            "Waiting 5s for the automation to be loaded by the triggers services"
        )
        await asyncio.sleep(5)

        try:
            yield automation
        finally:
            try:
                await prefect._client.delete(f"/automations/{automation['id']}")
            except PrefectHTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info("Automation %s already deleted", automation["id"])
                else:
                    raise


async def wait_for_deployment(deployment_name: str, timeout: float = 60) -> str:
    """Wait for a deployment to appear, return its ID."""
    logger = get_run_logger()
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)

    async with get_client() as prefect:
        while datetime.now(timezone.utc) < deadline:
            deployments = await prefect.read_deployments(
                deployment_filter=DeploymentFilter(
                    name=DeploymentFilterName(like_=deployment_name)
                )
            )
            if deployments:
                logger.info(
                    "Found deployment %s (%s)", deployment_name, deployments[0].id
                )
                return str(deployments[0].id)
            await asyncio.sleep(2)

    raise TimeoutError(f"Deployment {deployment_name} did not appear within {timeout}s")


async def trigger_flow_run(deployment_id: str) -> str:
    """Trigger a flow run via API, return its ID."""
    logger = get_run_logger()

    async with get_client() as prefect:
        flow_run = await prefect.create_flow_run_from_deployment(deployment_id)
        logger.info("Triggered flow run %s", flow_run.id)
        return str(flow_run.id)


async def wait_for_heartbeat_event(flow_run_id: str, timeout: float = 90) -> None:
    """Wait until we see at least one heartbeat event for the flow run."""
    logger = get_run_logger()
    logger.info("Waiting for heartbeat event for flow run %s...", flow_run_id)

    filter = EventFilter(
        occurred=EventOccurredFilter(since=datetime.now(timezone.utc)),
        event=EventNameFilter(name=["prefect.flow-run.heartbeat"]),
        resource=EventResourceFilter(id=[f"prefect.flow-run.{flow_run_id}"]),
    )

    async with get_events_subscriber(filter=filter) as subscriber:
        try:
            async with asyncio.timeout(timeout):
                async for event in subscriber:
                    logger.info("Received heartbeat event: %s", event)
                    return
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"No heartbeat event received for flow run {flow_run_id} within {timeout}s. "
                "Heartbeats may not be enabled."
            )


async def wait_for_flow_run_running(flow_run_id: str, timeout: float = 60) -> None:
    """Poll until flow run state is RUNNING."""
    logger = get_run_logger()
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)

    async with get_client() as prefect:
        while datetime.now(timezone.utc) < deadline:
            flow_run = await prefect.read_flow_run(flow_run_id)
            state_name = flow_run.state.name if flow_run.state else "Unknown"

            if flow_run.state and flow_run.state.is_running():
                logger.info("Flow run %s is now RUNNING", flow_run_id)
                return

            if flow_run.state and flow_run.state.is_final():
                raise Exception(
                    f"Flow run {flow_run_id} reached terminal state {state_name} "
                    "before entering RUNNING"
                )

            await asyncio.sleep(1)

    raise TimeoutError(
        f"Flow run {flow_run_id} did not transition to RUNNING within {timeout}s"
    )


async def wait_for_flow_run_crashed(flow_run_id: str, timeout: float = 150) -> None:
    """Poll until flow run state is CRASHED."""
    logger = get_run_logger()
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)

    async with get_client() as prefect:
        while datetime.now(timezone.utc) < deadline:
            flow_run = await prefect.read_flow_run(flow_run_id)
            state_name = flow_run.state.name if flow_run.state else "Unknown"
            logger.info("Flow run %s state: %s", flow_run_id, state_name)

            if flow_run.state and flow_run.state.is_crashed():
                logger.info("Flow run %s is now CRASHED", flow_run_id)
                return

            if flow_run.state and flow_run.state.is_final():
                raise Exception(
                    f"Flow run {flow_run_id} reached terminal state {state_name} "
                    "instead of CRASHED"
                )

            await asyncio.sleep(5)

    raise TimeoutError(
        f"Flow run {flow_run_id} did not transition to CRASHED within {timeout}s"
    )


async def delete_deployment(deployment_id: str) -> None:
    """Delete a deployment by ID."""
    logger = get_run_logger()
    async with get_client() as prefect:
        try:
            await prefect.delete_deployment(deployment_id)
            logger.info("Deleted deployment %s", deployment_id)
        except Exception as e:
            logger.warning("Failed to delete deployment %s: %s", deployment_id, e)


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def assess_zombie_flow_detection():
    """
    Test that the heartbeat-based zombie flow detection automation works correctly.

    This test:
    1. Spawns a subprocess running serve() with heartbeats enabled
    2. Triggers a flow run and waits for heartbeat events
    3. Creates a proactive automation to detect missing heartbeats
    4. SIGKILLs the subprocess (simulating a zombie - no cleanup possible)
    5. Verifies the automation marks the flow run as CRASHED
    """
    logger = get_run_logger()

    deployment_name = f"zombie-test-{uuid4()}"
    process = None
    deployment_id = None

    try:
        # Write the serve script to a temp file in the current directory
        # (serve() requires the script to be in a subpath of cwd)
        script_path = f".zombie-test-{uuid4()}.py"
        with open(script_path, "w") as f:
            f.write(SERVE_SCRIPT)

        try:
            # Start subprocess with heartbeat frequency configured
            logger.info(
                "Starting serve() subprocess with deployment %s", deployment_name
            )
            env = os.environ.copy()
            env["PREFECT_RUNNER_HEARTBEAT_FREQUENCY"] = str(HEARTBEAT_FREQUENCY)

            process = subprocess.Popen(
                [sys.executable, script_path, deployment_name],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Started subprocess with PID %s", process.pid)

            # Wait for deployment to appear
            logger.info("Waiting for deployment to appear...")
            deployment_id = await wait_for_deployment(deployment_name)

            # Trigger flow run
            flow_run_id = await trigger_flow_run(deployment_id)

            # Create automation targeting this specific flow run BEFORE waiting for
            # heartbeat, so the automation sees the heartbeat and arms itself
            async with create_or_replace_automation(
                {
                    "name": "zombie-detection-test",
                    "trigger": {
                        "posture": "Proactive",
                        "after": ["prefect.flow-run.heartbeat"],
                        "expect": [
                            "prefect.flow-run.heartbeat",
                            "prefect.flow-run.Completed",
                            "prefect.flow-run.Failed",
                            "prefect.flow-run.Cancelled",
                            "prefect.flow-run.Crashed",
                        ],
                        "match": {
                            "prefect.resource.id": f"prefect.flow-run.{flow_run_id}"
                        },
                        "for_each": ["prefect.resource.id"],
                        "threshold": 1,
                        "within": AUTOMATION_WINDOW,
                    },
                    "actions": [
                        {
                            "type": "change-flow-run-state",
                            "state": "CRASHED",
                            "message": "Flow run stopped sending heartbeats (zombie detection test)",
                        }
                    ],
                }
            ):
                # Wait for first heartbeat (confirms heartbeats are working and arms
                # the automation)
                await wait_for_heartbeat_event(flow_run_id)
                logger.info("Heartbeats confirmed working, automation is now armed")

                # Wait for flow run to be in RUNNING state
                await wait_for_flow_run_running(flow_run_id)

                # SIGKILL the subprocess (simulates zombie - no cleanup possible)
                logger.info("Sending SIGKILL to subprocess PID %s", process.pid)
                os.kill(process.pid, signal.SIGKILL)
                process.wait()
                logger.info("Subprocess terminated")

                # Wait for automation to detect missing heartbeat and mark as crashed
                logger.info(
                    "Waiting for automation to mark flow run as CRASHED "
                    "(window: %ss)...",
                    AUTOMATION_WINDOW,
                )
                await wait_for_flow_run_crashed(flow_run_id)

            logger.info("Zombie flow detection test PASSED")

        finally:
            # Clean up temp file
            try:
                os.unlink(script_path)
            except Exception:
                pass

    finally:
        # Clean up subprocess if still running
        if process and process.poll() is None:
            logger.info("Cleaning up subprocess...")
            try:
                os.kill(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            except Exception as e:
                logger.warning("Failed to kill subprocess: %s", e)

        # Clean up deployment
        if deployment_id:
            await delete_deployment(deployment_id)


if __name__ == "__main__":
    asyncio.run(assess_zombie_flow_detection())
