from concurrent.futures import Future
import os
from unittest.mock import patch

import pytest
from sklearn.externals.joblib import Memory

from cml_pipelines import Pipeline, task


class MyPipeline(Pipeline):
    @task()
    def add(self, a, b):
        return a + b

    def build(self):
        return self.add(1, 1)


class TestPipeline:
    @pytest.mark.parametrize("clear_cache", [True, False])
    def test_run(self, clear_cache):
        with patch.object(Memory, "clear") as clear_func:
            pipeline = MyPipeline(clear_cache)
            future = pipeline.run()
            assert isinstance(future, Future)
            assert future.result(timeout=1) == 2
            if clear_cache:
                assert clear_func.call_count == 1
