from concurrent.futures import Future
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
    def test_build_not_implemented(self):
        pipeline = Pipeline()
        with pytest.raises(NotImplementedError):
            pipeline.build()

    @pytest.mark.parametrize("clear_cache", [True, False])
    @pytest.mark.parametrize("block", [True, False])
    def test_run(self, clear_cache, block):
        with patch.object(Memory, "clear") as clear_func:
            pipeline = MyPipeline(clear_cache)
            result = pipeline.run(block=block)

            if not block:
                assert isinstance(result, Future)
                assert result.result(timeout=0.1) == 2
            else:
                assert isinstance(result, int)
                assert result == 2

            if clear_cache:
                assert clear_func.call_count == 1

    def test_visualize(self):
        pipeline = MyPipeline()
        pipeline.visualize()
