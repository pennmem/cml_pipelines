from cml_pipelines.wrapper import task, make_task


def test_decorator():
    @task()
    def my_task():
        return 1

    @task(log_args=True)
    def my_other_task(value):
        return value

    assert my_task().compute() == 1
    assert my_other_task(my_task()).compute() == 1


def test_make_task():
    def my_task(n):
        return n

    assert make_task(my_task, 1).compute() == 1
    assert my_task(1) == 1
