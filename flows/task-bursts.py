from prefect import flow, get_run_logger, task


@task
def say_hello(name: str):
    get_run_logger().info(f"Hello {name}!")


@flow
def burst_tasks(name: str = "world", count: int = 250):
    [task_fut.wait() for task_fut in say_hello.map(f"{name}-{i}" for i in range(count))]


if __name__ == "__main__":
    burst_tasks()
