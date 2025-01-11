from prefect import task

@task(name="Load")
def task_load(data):
    print(f"data saved to database :{data}")
