from concurrent.futures import Future
import socket
from unittest.mock import patch

import pytest
from sklearn.externals.joblib import Memory

from cml_pipelines import Pipeline, task
from cml_pipelines.pipeline import CLUSTER_DEFAULTS


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

    @pytest.mark.parametrize("cluster_kwargs", [None, {"cores": 4}])
    def test_run_cluster(self, cluster_kwargs):
        with patch("dask_jobqueue.SGECluster") as MockCluster:
            with patch("dask.distributed.Client") as MockClient:
                pipeline = MyPipeline()
                pipeline.run(cluster=True, cluster_kwargs=cluster_kwargs)

                if cluster_kwargs is None:
                    kwargs = CLUSTER_DEFAULTS
                else:
                    kwargs = CLUSTER_DEFAULTS.copy()
                    kwargs.update(cluster_kwargs)

                MockClient.assert_called()

                call_args = MockCluster.call_args
                assert call_args is not None
                args = call_args[1]

                for key, value in args.items():
                    assert key in kwargs
                    assert kwargs[key] == value

    @pytest.mark.skipif("rhino" not in socket.gethostname())
    def test_run_cluster_rhino(self):
        """Test running on the actual SGE cluster."""
        pipeline = MyPipeline()
        result = pipeline.run(cluster=True)
        assert result == 2

    def test_visualize(self):
        pipeline = MyPipeline()
        pipeline.visualize()
