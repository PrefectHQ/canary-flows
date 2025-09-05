import asyncio
import textwrap
from typing import Any

from prefect import flow, get_client, get_run_logger


async def create_or_replace_automation(automation: dict[str, Any]):
    logger = get_run_logger()

    async with get_client() as prefect:
        response = await prefect._client.post("/automations/filter")
        response.raise_for_status()
        for existing in response.json():
            if existing["name"] == automation["name"]:
                await prefect._client.delete(f"/automations/{existing['id']}")

        response = await prefect._client.post("/automations", json=automation)
        response.raise_for_status()

        automation = response.json()
        logger.info("Created automation %s", automation["id"])

        return automation


@flow
async def ensure_integrations_automations():
    NOTIFICATION_BLOCK = "237c3f19e-8c2d-48db-9783-68ea848bfa4f"

    await create_or_replace_automation(
        {
            "name": "integration-flows-failed",
            "description": (
                "Notifies when any flow run that is expected to succeed has failed "
                "[created by the integration-tests]"
            ),
            "trigger": {
                "posture": "Reactive",
                "expect": [
                    "prefect.flow-run.Failed",
                    "prefect.flow-run.Crashed",
                    "prefect.flow-run.TimedOut",
                ],
                "threshold": 1,
                "within": 0,
                "match_related": {
                    "prefect.resource.id": "prefect.tag.expected:success",
                    "prefect.resource.role": "tag",
                },
            },
            "actions": [
                {
                    "type": "send-notification",
                    "block_document_id": NOTIFICATION_BLOCK,
                    "subject": (
                        "Flow run "
                        "{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }} "
                        "{{ event.resource['prefect.state-name']|lower}} unexpectedly"
                    ),
                    "body": textwrap.dedent(
                        """
                        State message:

                        ```
                        {{ event.resource['prefect.state-message'] }}
                        ```

                        This integration flow run was expected to succeed but failed.
                        To investigate further:

                        * Flow run: [{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }}]({{ event.resource|ui_url }})
                        {% if event.resource_in_role['work-pool'] %}
                        * Work pool: [{{ event.resource_in_role['work-pool'].name }}]({{ event.resource_in_role['work-pool']|ui_url }})
                        {% endif %}
                    """
                    ).strip(),
                }
            ],
        }
    )

    await create_or_replace_automation(
        {
            "name": "integration-flows-succeeded",
            "description": (
                "Notifies when any flow run that is expected to fail has succeeded "
                "[created by the integration-tests]"
            ),
            "trigger": {
                "posture": "Reactive",
                "expect": ["prefect.flow-run.Completed"],
                "threshold": 1,
                "within": 0,
                "match_related": {
                    "prefect.resource.id": "prefect.tag.expected:failure",
                    "prefect.resource.role": "tag",
                },
            },
            "actions": [
                {
                    "type": "send-notification",
                    "block_document_id": NOTIFICATION_BLOCK,
                    "subject": (
                        "Flow run "
                        "{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }} "
                        "{{ event.resource['prefect.state-name']|lower}} unexpectedly"
                    ),
                    "body": textwrap.dedent(
                        """
                        State message:

                        ```
                        {{ event.resource['prefect.state-message'] }}
                        ```

                        This integration flow run was expected to fail, but it succeeded.
                        To investigate further:

                        * Flow run: [{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }}]({{ event.resource|ui_url }})
                        {% if event.resource_in_role['work-pool'] %}
                        * Work pool: [{{ event.resource_in_role['work-pool'].name }}]({{ event.resource_in_role['work-pool']|ui_url }})
                        {% endif %}
                    """
                    ).strip(),
                }
            ],
        }
    )

    await create_or_replace_automation(
        {
            "name": "integration-flows-stuck-pending",
            "description": (
                "Notifies when any flow run has been stuck Pending [created by the "
                "integration-tests]"
            ),
            "trigger": {
                "posture": "Proactive",
                "after": ["prefect.flow-run.Pending"],
                "expect": [
                    "prefect.flow-run.Running",
                    "prefect.flow-run.Crashed",
                    "prefect.flow-run.Failed",
                    "prefect.flow-run.Completed",
                    "prefect.flow-run.Paused",
                ],
                "for_each": ["prefect.resource.id"],
                "threshold": 1,
                "within": 300,
            },
            "actions": [
                {
                    "type": "send-notification",
                    "block_document_id": NOTIFICATION_BLOCK,
                    "subject": (
                        "Flow run "
                        "{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }} "
                        "stuck `Pending` for more than 5 minutes"
                    ),
                    "body": textwrap.dedent(
                        """
                        This integration flow run entered `Pending` but didn't move on
                        to `Running` after 5 minutes.

                        To investigate further:

                        * Flow run: [{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }}]({{ event.resource|ui_url }})
                        {% if event.resource_in_role['work-pool'] %}
                        * Work pool: [{{ event.resource_in_role['work-pool'].name }}]({{ event.resource_in_role['work-pool']|ui_url }})
                        {% endif %}
                    """
                    ).strip(),
                }
            ],
        }
    )

    await create_or_replace_automation(
        {
            "name": "integration-flows-stuck-running",
            "description": (
                "Notifies when any flow run has been stuck Running [created by the "
                "integration-tests]"
            ),
            "trigger": {
                "posture": "Proactive",
                "after": ["prefect.flow-run.Running"],
                "expect": [
                    "prefect.flow-run.Crashed",
                    "prefect.flow-run.Failed",
                    "prefect.flow-run.Completed",
                    "prefect.flow-run.Paused",
                    "prefect.flow-run.TimedOut",
                ],
                "for_each": ["prefect.resource.id"],
                "threshold": 1,
                "within": 900,
            },
            "actions": [
                {
                    "type": "send-notification",
                    "block_document_id": NOTIFICATION_BLOCK,
                    "subject": (
                        "Flow run "
                        "{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }} "
                        "stuck `Running` for more than 15 minutes"
                    ),
                    "body": textwrap.dedent(
                        """
                        This integration flow run entered `Running` but didn't move on
                        to a terminal state after 15 minutes.

                        To investigate further:

                        * Flow run: [{{ event.resource_in_role['flow'].name }}/{{ event.resource.name }}]({{ event.resource|ui_url }})
                        {% if event.resource_in_role['work-pool'] %}
                        * Work pool: [{{ event.resource_in_role['work-pool'].name }}]({{ event.resource_in_role['work-pool']|ui_url }})
                        {% endif %}
                    """
                    ).strip(),
                }
            ],
        }
    )

    await create_or_replace_automation(
        {
            "name": "integration-work-pool-not-ready",
            "description": (
                "Notifies when a work pool has stayed not-ready for too long [created by the "
                "integration-tests]"
            ),
            "trigger": {
                "posture": "Proactive",
                "after": ["prefect.work-pool.not-ready"],
                "expect": ["prefect.work-pool.ready"],
                "for_each": ["prefect.resource.id"],
                "threshold": 1,
                "within": 600,
            },
            "actions": [
                {
                    "type": "send-notification",
                    "block_document_id": NOTIFICATION_BLOCK,
                    "subject": "Work pool '{{ event.resource.name }}' is not ready",
                    "body": textwrap.dedent(
                        """
                        This work pool entered a `NOT_READY` status 10 minutes ago and
                        has not recovered yet.

                        To investigate further:

                        * Work pool: [{{ event.resource.name }}]({{ event.resource|ui_url }})
                    """
                    ).strip(),
                }
            ],
        }
    )


if __name__ == "__main__":
    asyncio.run(ensure_integrations_automations())
