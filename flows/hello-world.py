from prefect import flow, get_run_logger


@flow
def hello_world_entry(name: str = "world"):
    get_run_logger().info(f"Hello {name}!")


if __name__ == "__main__":
    hello_world_entry()
