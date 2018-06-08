"""Module for configuring the global cache to be used by tasks in a pipeline.
By default, the cache directory will be located in a directory named
``cml_pipelines_cache`` placed in the current working directory.

This module exports a ``memory`` object. To set the cache directory::

    >>> memory.cachedir = "/tmp/joblib"

To clear the cache::

    >>> memory.clear()

"""

import os
from sklearn.externals import joblib

__all__ = ["memory"]

_cachedir = os.path.join(os.getcwd(), "cml_pipelines_cache")

try:
    os.makedirs(_cachedir)
except OSError:
    pass

memory = joblib.Memory(cachedir=_cachedir, verbose=0)
