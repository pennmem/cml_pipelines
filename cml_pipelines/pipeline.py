from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Union

from dask.delayed import Delayed

from .cache import memory


class Pipeline(object):
    """Base class for building pipelines.

    Parameters
    ----------
    clear_cache_on_completion
        When True (the default), clear the cache upon successful completion.

    """
    def __init__(self, clear_cache_on_completion: bool = True):
        self.clear_cache_on_completion = clear_cache_on_completion

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
            if self.clear_cache_on_completion:
                future.add_done_callback(lambda f: memory.clear(warn=False))
            return future

    def _run_sync(self):
        pipeline = self.build()
        result = pipeline.compute()

        if self.clear_cache_on_completion:
            memory.clear(warn=False)

        return result

    def run(self, block=True) -> Union[Future, Any]:
        """Run the pipeline.

        Parameters
        ----------
        block
            When True (the default), block until completion. Otherwise, return
            a :class:`Future`.

        Returns
        -------
        If ``block`` is set, returns the result of running the pipeline.
        Otherwise returns a :class:`Future` which resolves when the pipeline
        is complete.

        """
        if not block:
            return self._run_async()
        else:
            return self._run_sync()
