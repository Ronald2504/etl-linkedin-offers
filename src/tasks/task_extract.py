from prefect import task

@task(name="Extract")
def task_extract():
    data = [{'id': 1, 'name': 'John Doe'}, {'id': 2, 'name': 'Jane Doe'}]
    return data
