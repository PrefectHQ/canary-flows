# Canary Flows

Canary flows are integration test flows that the Prefect team uses to smoke test nightly releases of `prefect`. These flows run continuously against bleeding-edge Prefect versions to catch regressions and ensure core functionality works as expected.

## Purpose

This repository contains a collection of Prefect flows that exercise various Prefect features and integrations. Each flow is designed to either succeed or fail in predictable ways, allowing automated monitoring to detect when Prefect functionality breaks.

## Setup

Install dependencies:

```bash
uv sync
```

## Adding New Integration Test Flows

To add a new integration test flow:

1. **Create the flow file** in the `flows/` directory with a descriptive name (e.g., `flows/new-feature-test.py`)

2. **Implement your flow** following these patterns:
   ```python
   from prefect import flow, task
   
   @task
   def test_specific_functionality():
       # Test implementation
       pass
   
   @flow
   def new_feature_test_entry():
       """Entry point for new feature integration test."""
       test_specific_functionality()
       return "success"
   ```

3. **Add deployment configuration** to `prefect.yaml`:
   ```yaml
   - name: New Feature Test
     tags:
       - expected:success  # or expected:failure
     description: *description
     schedule: *every_five_minutes
     entrypoint: flows/new-feature-test.py:new_feature_test_entry
     work_pool: *integration-tests-work-pool
   ```

4. **Choose appropriate tags**:
   - `expected:success` - Flow should complete successfully
   - `expected:failure` - Flow is expected to fail (for testing error handling)

5. **Select scheduling**:
   - `*every_five_minutes` - Standard integration test frequency
   - `*every_three_hours` - For less frequent maintenance tasks
   - Custom intervals for specific needs

## Deployment

Before deploying, ensure you're connected to the correct Prefect server.

Then deploy all flows:

```bash
uv run prefect deploy --all
```

This command:
- Reads the `prefect.yaml` configuration
- Creates deployments for all flows defined in the deployments section
- Registers them with your Prefect server/cloud instance
- Schedules them according to their configured schedules

### Prerequisites

Before deploying, ensure:
- Dependencies are installed (`uv sync`)
- Prefect is authenticated (see above)
- The `integration-tests` work pool exists and is configured
- Required environment variables and secrets are available

### Selective Deployment

To deploy specific flows, use names:

```bash
# Deploy flows with specific names
uv run prefect deploy -n "Hello World" -n "Task Retries"
```
