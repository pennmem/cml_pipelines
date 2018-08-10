from concurrent.futures import Future
import logging
from unittest.mock import patch

from dask import delayed
import pytest

from cml_pipelines.pipeline import Pipeline, CLUSTER_DEFAULTS


class MyPipeline(Pipeline):
    @delayed
    def add(self, a, b):
        return a + b

    def build(self):
        return self.add(1, 1)


class SinkPipeline(MyPipeline):
    def __init__(self, return_all):
        self.return_all = return_all

    def build(self):
        sums = [self.add(a, a + 1) for a in range(10)]
        return self.sink(sums, self.return_all)


class TestPipeline:
    def test_build_not_implemented(self):
        pipeline = Pipeline()
        with pytest.raises(NotImplementedError):
            pipeline.build()

    @pytest.mark.parametrize("block", [True, False])
    def test_run(self, block):
        pipeline = MyPipeline()
        result = pipeline.run(block=block)

        if not block:
            assert isinstance(result, Future)
            assert result.result(timeout=0.1) == 2
        else:
            assert isinstance(result, int)
            assert result == 2

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

                assert MockClient.call_count == 1

                call_args = MockCluster.call_args
                assert call_args is not None
                args = call_args[1]

                for key, value in args.items():
                    assert key in kwargs
                    assert kwargs[key] == value

    # FIXME: make this work by including tests in the cml_pipelines package
    # @pytest.mark.skipif("rhino" not in socket.gethostname(),
    #                     reason="not running tests on rhino")
    # def test_run_cluster_rhino(self):
    #     """Test running on the actual SGE cluster."""
    #     pipeline = MyPipeline()
    #     result = pipeline.run(cluster=True)
    #     assert result == 2

    @pytest.mark.parametrize("debug", [True, False])
    def test_run_debug(self, debug):
        with patch.object(MyPipeline, "_run_sync") as run_sync:
            pipeline = MyPipeline()
            pipeline.run(debug=debug)
            run_sync.assert_called_with(debug)

    def test_visualize(self):
        pipeline = MyPipeline()
        pipeline.visualize()

    @pytest.mark.parametrize("return_all", [True, False])
    def test_sink(self, return_all):
        pipeline = SinkPipeline(return_all)
        results = pipeline.run()

        if return_all:
            assert isinstance(results, list)
            assert len(results) == 10
            for i, value in enumerate(results):
                assert value == i + i + 1
        else:
            assert results is None

    def test_get_logger(self):
        from cml_pipelines.log import ZMQLogHandler

        pipeline = MyPipeline()
        logger = pipeline.get_logger("tcp://127.0.0.1:9897")
        assert isinstance(logger, logging.Logger)
        assert logger.name == pipeline.pipeline_id
        zmq_handler = [isinstance(h, ZMQLogHandler) for h in logger.handlers]
        assert any(zmq_handler)
