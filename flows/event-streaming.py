import asyncio
from uuid import uuid4

import pendulum
from prefect import flow, get_client, get_run_logger
from prefect.events import Event, Resource
from prefect.events.clients import get_events_client, get_events_subscriber
from prefect.events.filters import (
    EventFilter,
    EventNameFilter,
    EventOccurredFilter,
    EventResourceFilter,
)


async def wait_for_event(listening: asyncio.Event, filter: EventFilter) -> Event:
    logger = get_run_logger()

    logger.info("Starting event subscriber...")

    async with get_events_subscriber(filter=filter) as subscriber:
        logger.info("Subscribed and waiting for events...")
        listening.set()
        async for event in subscriber:
            logger.info(event)
            return event

    raise Exception("Disconnected without an event")


@flow
async def event_streaming():
    logger = get_run_logger()

    expected_resource = Resource(
        {"prefect.resource.id": f"integration:streaming:{uuid4()}"}
    )

    filter = EventFilter(
        occurred=EventOccurredFilter(since=pendulum.now("UTC")),
        event=EventNameFilter(name=["integration.example.event"]),
        resource=EventResourceFilter(id=[expected_resource.id]),
    )

    listening = asyncio.Event()
    listener = asyncio.create_task(wait_for_event(listening, filter))
    await listening.wait()

    logger.info("Emitting example events for %r...", expected_resource)
    async with get_events_client() as events:
        await events.emit(
            Event(
                event="integration.example.event",
                resource=expected_resource,
            )
        )

    allowance = 60

    try:
        async with asyncio.timeout(allowance):
            await listener
    except asyncio.TimeoutError:
        message = (
            f"Event was not received within {allowance}s, but it should be nearly "
            "instantaneous. "
        )
        async with get_client() as client:
            response = await client._client.post(
                "/events/filter",
                json={
                    "filter": filter.model_dump(mode="json"),
                },
            )
            if response.status_code != 200:
                message += (
                    "When trying to retrieve events via the API, we got "
                    f"a non-200 status code {response.status_code}. "
                )
            else:
                page = response.json()
                if page["total"] == 0:
                    message += (
                        "It wasn't visible on the /events/filter endpoint either, "
                        "which means there could be a problem with the inbound event "
                        "websocket /events/in. "
                    )
                else:
                    message += (
                        "It was visible on the /events/filter endpoint, which means "
                        "there could be a problem with the outbound event websocket "
                        "/events/out. "
                    )

            message += (
                "This is likely to cause flakes with the automations integration "
                "tests, so diagnose this problem first."
            )

            raise Exception(message)


if __name__ == "__main__":
    asyncio.run(event_streaming())
