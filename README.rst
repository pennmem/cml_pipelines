CML Pipelines
=============

.. image:: https://img.shields.io/travis/pennmem/cml_pipelines.svg
   :target: https://travis-ci.org/pennmem/cml_pipelines

.. image:: https://codecov.io/gh/pennmem/cml_pipelines/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/pennmem/cml_pipelines

.. image:: https://img.shields.io/badge/docs-here-brightgreen.svg
   :target: https://pennmem.github.io/pennmem/cml_pipelines/html/index.html
   :alt: docs

Utilities for constructing pipelines with dask.

Usage example
-------------

.. code-block:: python

    from functools import reduce
    import operator
    import random

    from cml_pipelines import Pipeline, task, make_task


    class MyPipeline(Pipeline):
        # Define a task using the task decorator
        @task(cache=False)
        def generate_datapoint(self):
            """Generate a single data point."""
            return random.random()

        def build(self):
            """Build the pipeline."""
            data = [self.generate_datapoint() for _ in range(1000)]

            # use make_task to wrap an existing function as a task
            total = make_task(reduce, operator.add, data)

            # return the final Delayed instance
            return total


    # run the pipeline
    pipeline = MyPipeline()
    future = pipeline.run()
    print(future.result())
