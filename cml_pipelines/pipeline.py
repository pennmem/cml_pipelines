from concurrent.futures import Future, ThreadPoolExecutor
from getpass import getuser
import os
from typing import Any, Union

from dask.delayed import Delayed

CLUSTER_DEFAULTS = {
    "queue": "RAM.q",
    "memory": "8G",
    "cores": 2,
    "walltime": "12:00:00",
    "local_directory": os.path.join("/", "scratch", getuser(), "dask")
}


class Pipeline(object):
    """Base class for building pipelines."""
    # dask instances for running on the cluster
    cluster = None
    client = None

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

    def _run_async(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            pipeline = self.build()
            future = executor.submit(pipeline.compute)
            return future

    def _run_sync(self):
        pipeline = self.build()
        result = pipeline.compute()
        return result

    def run(self, block: bool = True,
            cluster: bool = False,
            cluster_kwargs: dict = None,
            workers: int = 8) -> Union[Future, Any]:
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

        Returns
        -------
        If ``block`` is set, returns the result of running the pipeline.
        Otherwise returns a :class:`Future` which resolves when the pipeline
        is complete.

        """
        if cluster:
            from dask_jobqueue import SGECluster
            from dask.distributed import Client

            if cluster_kwargs is None:
                kwargs = CLUSTER_DEFAULTS
            else:
                kwargs = CLUSTER_DEFAULTS.copy()
                kwargs.update(cluster_kwargs)

            self.cluster = SGECluster(**kwargs)
            self.cluster.scale(workers)
            self.client = Client(self.cluster)

        if not block:
            return self._run_async()
        else:
            return self._run_sync()
