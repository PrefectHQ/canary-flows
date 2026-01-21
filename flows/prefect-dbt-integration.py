"""
prefect-dbt integration canary flow

exercises the dbt CLI tasks to catch regressions from async dispatch changes.
see: https://github.com/PrefectHQ/prefect/pull/20300
"""

from pathlib import Path

from prefect import flow, get_run_logger
from prefect_dbt.cli.commands import (
    run_dbt_build,
    run_dbt_model,
    run_dbt_seed,
    run_dbt_test,
    trigger_dbt_cli_command,
)

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"
DBT_PROFILES_DIR = DBT_PROJECT_DIR / "profiles"


@flow
def prefect_dbt_integration_entry():
    """
    exercises prefect-dbt CLI tasks:
    - trigger_dbt_cli_command (core task)
    - run_dbt_seed
    - run_dbt_model
    - run_dbt_build
    - run_dbt_test
    """
    logger = get_run_logger()
    project_dir = str(DBT_PROJECT_DIR)
    profiles_dir = str(DBT_PROFILES_DIR)

    logger.info(f"dbt project: {project_dir}")
    logger.info(f"dbt profiles: {profiles_dir}")

    # 1. test trigger_dbt_cli_command directly with debug
    logger.info("running: dbt debug")
    trigger_dbt_cli_command(
        command="dbt debug",
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    # 2. seed the data
    logger.info("running: dbt seed")
    run_dbt_seed(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    # 3. run the model
    logger.info("running: dbt run")
    run_dbt_model(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    # 4. run tests (will pass even with no explicit tests - dbt returns success)
    logger.info("running: dbt test")
    run_dbt_test(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    # 5. run full build (seed + run + test)
    logger.info("running: dbt build")
    run_dbt_build(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    logger.info("prefect-dbt integration tests passed")


if __name__ == "__main__":
    prefect_dbt_integration_entry()
