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

    from cml_pipelines import task, make_task


    # Define a task using the task decorator
    @task(cache=False)
    def generate_datapoint():
        """Generate a single data point."""
        return random.random()


    # build pipeline
    data = [generate_datapoint() for _ in range(1000)]

    # use make_task to wrap an existing function as a task
    total = make_task(reduce, operator.add, data)

    # get the result
    print(total.compute())
