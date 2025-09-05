import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator
from uuid import uuid4

import pendulum
from prefect import flow, get_client, get_run_logger
from prefect.concurrency.asyncio import concurrency
from prefect.events import Event
from prefect.events.clients import get_events_client, get_events_subscriber
from prefect.events.filters import (
    EventFilter,
    EventNameFilter,
    EventOccurredFilter,
    EventResourceFilter,
)

# The integration tests are scheduled to run every 5 minutes, so we should be timing
# out more quickly than that to avoid multiple runs stacking up
INTEGRATION_TEST_INTERVAL = 5 * 60
INTEGRATION_TEST_TIMEOUT = INTEGRATION_TEST_INTERVAL - 60
EVENT_TIMEOUT = INTEGRATION_TEST_TIMEOUT - 60


@asynccontextmanager
async def create_or_replace_automation(
    automation: dict[str, Any],
) -> AsyncGenerator[dict[str, Any], None]:
    logger = get_run_logger()

    async with get_client() as prefect:
        # Clean up any older automations with the same name prefix
        response = await prefect._client.post("/automations/filter")
        response.raise_for_status()
        for existing in response.json():
            name = str(existing["name"])
            if name.startswith(automation["name"]):
                age = pendulum.now("UTC") - pendulum.parse(existing["created"])
                assert isinstance(age, timedelta)
                if age > timedelta(seconds=INTEGRATION_TEST_INTERVAL * 3):
                    logger.info(
                        "Deleting old automation %s (%s)",
                        existing["name"],
                        existing["id"],
                    )
                await prefect._client.delete(f"/automations/{existing['id']}")

        automation["name"] = f"{automation['name']}:{uuid4()}"

        response = await prefect._client.post("/automations", json=automation)
        response.raise_for_status()

        automation = response.json()
        logger.info("Created automation %s (%s)", automation["name"], automation["id"])

        logger.info("Waiting 5s for the automation to be loaded the triggers services")
        await asyncio.sleep(5)

        try:
            yield automation
        finally:
            response = await prefect._client.delete(f"/automations/{automation['id']}")
            if response.status_code == 404:
                logger.info("Automation %s already deleted", automation["id"])
                return
            response.raise_for_status()


async def wait_for_event(
    listening: asyncio.Event, event_name: str, resource_id: str
) -> Event:
    logger = get_run_logger()

    logger.info("Starting event subscriber...")

    filter = EventFilter(
        occurred=EventOccurredFilter(since=pendulum.now("UTC")),
        event=EventNameFilter(name=[event_name]),
        resource=EventResourceFilter(id=[resource_id]),
    )
    async with get_events_subscriber(filter=filter) as subscriber:
        logger.info("Subscribed and waiting for events...")
        listening.set()
        async for event in subscriber:
            logger.info(event)
            return event

    raise Exception("Disconnected without an event")


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def assess_reactive_automation():
    logger = get_run_logger()
    async with concurrency(
        "assess_reactive_automation", timeout_seconds=INTEGRATION_TEST_TIMEOUT
    ):
        expected_resource = {"prefect.resource.id": f"integration:reactive:{uuid4()}"}
        async with create_or_replace_automation(
            {
                "name": "reactive-automation",
                "trigger": {
                    "posture": "Reactive",
                    "expect": ["integration.example.event"],
                    "match": expected_resource,
                    "threshold": 5,
                    "within": 60,
                },
                "actions": [{"type": "do-nothing"}],
            }
        ) as automation:
            listening = asyncio.Event()
            listener = asyncio.create_task(
                wait_for_event(
                    listening,
                    "prefect-cloud.automation.triggered",
                    f"prefect-cloud.automation.{automation['id']}",
                )
            )
            await listening.wait()

            logger.info("Emitting example events for %r...", expected_resource)
            async with get_events_client() as events:
                for i in range(5):
                    await events.emit(
                        Event(
                            event="integration.example.event",
                            resource=expected_resource,
                            payload={"iteration": i},
                        )
                    )

            logger.info("Waiting %ss for automation to fire...", EVENT_TIMEOUT)
            # Wait until we see the automation triggered event, or fail if it takes
            # longer than EVENT_TIMEOUT.  The reactive trigger should fire almost
            # immediately, and the event may take a little bit of time to be seen on
            # the event stream.
            try:
                async with asyncio.timeout(EVENT_TIMEOUT):
                    await listener
            except asyncio.TimeoutError:
                raise Exception(
                    f"Reactive automation did not trigger within {EVENT_TIMEOUT}s"
                )


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def assess_proactive_automation():
    logger = get_run_logger()
    async with concurrency(
        "assess_proactive_automation", timeout_seconds=INTEGRATION_TEST_TIMEOUT
    ):
        expected_resource = {"prefect.resource.id": f"integration:proactive:{uuid4()}"}
        async with create_or_replace_automation(
            {
                "name": "proactive-automation",
                "trigger": {
                    "posture": "Proactive",
                    "expect": ["integration.example.event"],
                    # Doing it for_each resource ID should prevent it from firing
                    # endlessly while the integration tests are _not_ running
                    "for_each": ["prefect.resource.id"],
                    "match": expected_resource,
                    "threshold": 5,
                    "within": 15,
                },
                "actions": [{"type": "do-nothing"}],
            }
        ) as automation:
            listening = asyncio.Event()
            listener = asyncio.create_task(
                wait_for_event(
                    listening,
                    "prefect-cloud.automation.triggered",
                    f"prefect-cloud.automation.{automation['id']}",
                )
            )
            await listening.wait()

            logger.info("Emitting example events for %r...", expected_resource)
            async with get_events_client() as events:
                for i in range(2):  # not enough events to close the automation
                    await events.emit(
                        Event(
                            event="integration.example.event",
                            resource=expected_resource,
                            payload={"iteration": i},
                        )
                    )

            logger.info("Waiting %ss for automation to fire...", EVENT_TIMEOUT)
            # Wait until we see the automation triggered event, or fail if it takes
            # longer than EVENT_TIMEOUT.  The proactive trigger should take a little over
            # 15s to fire, and the event may take a little bit of time to be seen on
            # the event stream.
            try:
                async with asyncio.timeout(EVENT_TIMEOUT):
                    await listener
            except asyncio.TimeoutError:
                raise Exception(
                    f"Proactive automation did not trigger within {EVENT_TIMEOUT}s"
                )


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def assess_compound_automation():
    logger = get_run_logger()
    async with concurrency(
        "assess_compound_automation", timeout_seconds=INTEGRATION_TEST_TIMEOUT
    ):
        expected_resource = {"prefect.resource.id": f"integration:compound:{uuid4()}"}
        async with create_or_replace_automation(
            {
                "name": "compound-automation",
                "trigger": {
                    "type": "compound",
                    "require": "all",
                    "within": 60,
                    "triggers": [
                        {
                            "posture": "Reactive",
                            "expect": ["integration.example.event.A"],
                            "match": expected_resource,
                            "threshold": 1,
                            "within": 0,
                        },
                        {
                            "posture": "Reactive",
                            "expect": ["integration.example.event.B"],
                            "match": expected_resource,
                            "threshold": 1,
                            "within": 0,
                        },
                    ],
                },
                "actions": [{"type": "do-nothing"}],
            }
        ) as automation:
            listening = asyncio.Event()
            listener = asyncio.create_task(
                wait_for_event(
                    listening,
                    "prefect-cloud.automation.triggered",
                    f"prefect-cloud.automation.{automation['id']}",
                )
            )
            await listening.wait()

            logger.info("Emitting example events for %r...", expected_resource)
            async with get_events_client() as events:
                await events.emit(
                    Event(
                        event="integration.example.event.A",
                        resource=expected_resource,
                    )
                )
                await events.emit(
                    Event(
                        event="integration.example.event.B",
                        resource=expected_resource,
                    )
                )

            logger.info("Waiting %ss for automation to fire...", EVENT_TIMEOUT)
            # Wait until we see the automation triggered event, or fail if it takes
            # longer than EVENT_TIMEOUT.  The compound trigger should fire almost
            # immediately, and the event may take a little bit of time to be seen on
            # the event stream.
            try:
                async with asyncio.timeout(EVENT_TIMEOUT):
                    await listener
            except asyncio.TimeoutError:
                raise Exception(
                    f"Compound automation did not trigger within {EVENT_TIMEOUT}s"
                )


@flow(timeout_seconds=INTEGRATION_TEST_TIMEOUT)
async def assess_sequence_automation():
    logger = get_run_logger()
    async with concurrency(
        "assess_sequence_automation", timeout_seconds=INTEGRATION_TEST_TIMEOUT
    ):
        expected_resource = {"prefect.resource.id": f"integration:sequence:{uuid4()}"}
        async with create_or_replace_automation(
            {
                "name": "sequence-automation",
                "trigger": {
                    "type": "sequence",
                    "within": 60,
                    "triggers": [
                        {
                            "posture": "Reactive",
                            "expect": ["integration.example.event.A"],
                            "match": expected_resource,
                            "threshold": 1,
                            "within": 0,
                        },
                        {
                            "posture": "Reactive",
                            "expect": ["integration.example.event.B"],
                            "match": expected_resource,
                            "threshold": 1,
                            "within": 0,
                        },
                    ],
                },
                "actions": [{"type": "do-nothing"}],
            }
        ) as automation:
            listening = asyncio.Event()
            listener = asyncio.create_task(
                wait_for_event(
                    listening,
                    "prefect-cloud.automation.triggered",
                    f"prefect-cloud.automation.{automation['id']}",
                )
            )
            await listening.wait()

            logger.info("Emitting example events for %r...", expected_resource)
            first = uuid4()
            second = uuid4()
            async with get_events_client() as events:
                await events.emit(
                    Event(
                        id=first,
                        event="integration.example.event.A",
                        resource=expected_resource,
                    )
                )

            get_run_logger().info("Waiting 5s to make sure the sequence is unambiguous")
            await asyncio.sleep(5)

            async with get_events_client() as events:
                await events.emit(
                    Event(
                        id=second,
                        follows=first,
                        event="integration.example.event.B",
                        resource=expected_resource,
                    )
                )

            logger.info("Waiting %ss for automation to fire...", EVENT_TIMEOUT)
            # Wait until we see the automation triggered event, or fail if it takes
            # longer than EVENT_TIMEOUT.  The compound trigger should fire almost
            # immediately, and the event may take a little bit of time to be seen on
            # the event stream.
            try:
                async with asyncio.timeout(EVENT_TIMEOUT):
                    await listener
            except asyncio.TimeoutError:
                raise Exception(
                    f"Sequence automation did not trigger within {EVENT_TIMEOUT}s"
                )


if __name__ == "__main__":
    asyncio.run(assess_reactive_automation())
    asyncio.run(assess_proactive_automation())
    asyncio.run(assess_compound_automation())
    asyncio.run(assess_sequence_automation())
