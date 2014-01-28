from celery import Celery, group
app = Celery('demo', broker='redis://localhost:6379/0')

app.conf.update(
    CELERY_RESULT_BACKEND='redis://localhost:6379/0',
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json')

@app.task
def add(a, b):
    return a + b

if __name__ == '__main__':
    tasks = []
    for _ in range(100):
        t = add.si(_, _/2)
        tasks.append(t)

    g = group(tasks)
    print g.apply_async().join()
