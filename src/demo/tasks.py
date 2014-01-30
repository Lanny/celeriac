from celery import Celery, group, chain
app = Celery('demo', broker='redis://localhost:6379/0')

app.conf.update(
    CELERY_RESULT_BACKEND='redis://localhost:6379/0',
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json')

@app.task(name="tasks.test_task")
def test_task(a):
    return 42

@app.task
def add(a, b):
    raise NotImplementedError()

@app.task
def slow_add(a, b):
    raise NotImplementedError()

@app.task
def make_japaneese(s):
    raise NotImplementedError()

@app.task
def make_spanish(s):
    raise NotImplementedError()

if __name__ == '__main__':
    #s = "I'm not sure this is how languages work"
    #result = chain(make_spanish.subtask(args=(s,), queue="celery"), 
    #    make_japaneese.subtask(queue="celery"))().get()

    #print result
    g = group(*[slow_add.si(1, 2) for _ in range(10)])
    print g().get()

