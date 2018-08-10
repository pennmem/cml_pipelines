from concurrent.futures import Future, ThreadPoolExecutor
from getpass import getuser
import logging
import os
from typing import Any, List, Union
from uuid import uuid4

from dask.delayed import Delayed, delayed

CLUSTER_DEFAULTS = {
    "queue": "RAM.q",
    "memory": "8G",
    "cores": 2,
    "walltime": "12:00:00",
    "local_directory": os.path.join("/", "scratch", getuser(), "dask")
}


class Pipeline(object):
    """Base class for building pipelines."""
    _handler = None  # type: logging.Handler
    _logger = None  # type: logging.Logger

    # An identifier to give the pipeline.
    pipeline_id = uuid4().hex

    def get_logger(self, address: str,
                   level: int = logging.INFO) -> logging.Logger:
        """Get a logger and configure it to send log records along a ZMQ
        socket. See the :mod:`cml_pipelines.log` module for details.

        Parameters
        ----------
        address
            The ZMQ address for the log consumer.
        level
            Log level to use.

        """
        if self._logger is None:
            from .log import ZMQLogHandler
            self._handler = ZMQLogHandler(address, level=level)
            self._logger = logging.getLogger(self.pipeline_id)
            self._logger.addHandler(self._handler)

        return self._logger

    def build(self) -> Delayed:
        """Override this method to define a pipeline. This method must return a
        :class:`Delayed` instance. This is most easily accomplished by returning
        the result of a function wrapped with the ``task`` decorator.

        """
        raise NotImplementedError

    def visualize(self, *args, **kwargs):
        """Use graphviz to visualize the task graph.

        Notes
        -----
        This method requires that ``graphviz`` is installed on your machine and
        that the ``python-graphviz`` package is installed. This can be done by
        running::

            $ conda install -c conda-forge python-graphviz

        """
        try:
            self.build().visualize(*args, **kwargs)
        except RuntimeError:  # pragma: nocover
            raise RuntimeError("Please install graphviz and python-graphviz")

    @delayed
    def sink(self, results: List[Delayed],
             return_all: bool = False) -> Union[None, list]:
        """Generic sink function for returning a single :class:`Delayed`
        instance from :meth:`build`.

        A common pattern might result in several calculations being run in
        parallel without a final step required to reduce to a single result, so
        this method can be used to help with that:

        .. code:: python

            def build(self):
                results = [self.run_task(x) for x in range(100)]
                return self.sink(results)

        Parameters
        ----------
        results
            :class:`Delayed` instances to sink.
        return_all
            When set, all accumulated results will be returned. When not (the
            default), return ``None``.

        """
        if return_all:
            return results

    def _run_async(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            pipeline = self.build()
            future = executor.submit(pipeline.compute)
            return future

    def _run_sync(self, debug: bool):
        pipeline = self.build()
        kwargs = {"scheduler": "single-threaded"} if debug else {}
        result = pipeline.compute(**kwargs)
        return result

    def run(self, block: bool = True,
            cluster: bool = False,
            cluster_kwargs: dict = None,
            workers: int = 8,
            debug: bool = False) -> Union[Future, Any]:
        """Run the pipeline.

        Parameters
        ----------
        block
            When True (the default), block until completion. Otherwise, return
            a :class:`Future`.
        cluster
            When True, run on rhino's SGE cluster (default: False).
        cluster_kwargs
            A dict of keyword arguments to pass to :class:`SGECluster`. See
            ``CLUSTER_DEFAULTS`` for default values.
        workers
            Number of workers to use when running on the SGE cluster
            (default: 8).
        debug
            When True, disable the cluster and use the single-threaded dask
            scheduler for debugging.

        Returns
        -------
        If ``block`` is set, returns the result of running the pipeline.
        Otherwise returns a :class:`Future` which resolves when the pipeline
        is complete.

        """
        if cluster and not debug:
            from dask_jobqueue import SGECluster
            from dask.distributed import Client

            if cluster_kwargs is None:
                kwargs = CLUSTER_DEFAULTS
            else:
                kwargs = CLUSTER_DEFAULTS.copy()
                kwargs.update(cluster_kwargs)

            cluster = SGECluster(**kwargs)
            cluster.scale(workers)
            _ = Client(cluster)

        if not block and not debug:
            return self._run_async()
        else:
            return self._run_sync(debug)
