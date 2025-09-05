from prefect import flow, get_run_logger


@flow
def automations_tracer_entry(name: str = "world"):
    get_run_logger().info(f"Hello {name}!")


if __name__ == "__main__":
    automations_tracer_entry()
