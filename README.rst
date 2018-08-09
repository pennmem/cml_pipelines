CML Pipelines
=============

.. image:: https://img.shields.io/travis/pennmem/cml_pipelines.svg
   :target: https://travis-ci.org/pennmem/cml_pipelines

.. image:: https://codecov.io/gh/pennmem/cml_pipelines/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/pennmem/cml_pipelines

.. image:: https://img.shields.io/badge/docs-here-brightgreen.svg
   :target: https://pennmem.github.io/pennmem/cml_pipelines/html/index.html
   :alt: docs

Build pipelines with dask and optionally run on the CML SGE cluster.

Usage
-----

Running locally:

.. code:: python

    from functools import reduce
    import operator
    import random

    from dask import delayed

    from cml_pipelines import Pipeline


    class MyPipeline(Pipeline):
        # Define a task using the delayed decorator
        @delayed
        def generate_datapoint(self):
            """Generate a single data point."""
            return random.random()

        def build(self):
            """Build the pipeline."""
            data = [self.generate_datapoint() for _ in range(1000)]

            # inline the delayed function to make an existing function into a task
            total = delayed(reduce)(operator.add, data)

            # return the final Delayed instance
            return total


    # run the pipeline
    pipeline = MyPipeline()
    result = pipeline.run()
    print(result)

To run on the cluster, pass ``cluster=True`` to ``pipeline.run``.
