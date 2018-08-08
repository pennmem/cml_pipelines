Changes
=======

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
