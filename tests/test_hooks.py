from threading import Lock

from cml_pipelines.hooks import PipelineCallback, PipelineStatusListener
from cml_pipelines.wrapper import task


class Counter:
    def __init__(self, start=0, step=1):
        self._lock = Lock()
        self._current = start
        self._step = step

    def increment(self):
        with self._lock:
            self._current += self._step

    @property
    def count(self):
        with self._lock:
            return self._current


def test_pipeline_callback():
    results = []

    @task()
    def my_task():
        return 0

    with PipelineStatusListener(lambda res: results.append(res)):
        with PipelineCallback('name'):
            my_task().compute()
            assert len(results)
