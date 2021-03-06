Changes
=======

Version 2.0.0
-------------

**2018-08-09**

* Support for caching has been removed. While this feature was potentially
  useful, the ``joblib`` implementation was sometimes causing more trouble than
  it was worth. A modified approach to caching may be added in a future
  release.
* The ``task`` decorator and friends have been removed. Use the ``dask.delayed``
  decorator directly.
* Support for running pipelines on rhino's SGE cluster was added.

Version 1.1.1
-------------

**2018-06-19**

Bug Fixes:

* ``wrapper.make_task()`` now accepts the same keyword arguments as ``wrapper.task()``;
  was previously missing ``nout`` argument

Version 1.1.0
-------------
**2018-05-17**

* Adds helper container class for keeping track of file paths relative to some
  mount point

Version 1.0.0
-------------

**2018-05-16**

This is the initial release which includes a decorator for making Dask delayed
tasks and hooks for broadcasting status updates. This functionality was
separated from ``ramutils`` in order to be reusable in other projects.
