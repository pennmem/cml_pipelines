import functools
import logging
from tempfile import gettempdir

from dask import delayed
from sklearn.externals import joblib

memory = joblib.Memory(cachedir=gettempdir(), verbose=0)
logger = logging.getLogger('cml.pipelines')


def _log_call(func, with_args=True):
    """Logs calling of a function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if with_args:
            logger.info("calling %s with args=%r, kwargs=%r",
                        func.__name__, args, kwargs)
        else:
            logger.info("calling %s", func.__name__)
        return func(*args, **kwargs)
    return wrapper


def task(cache=True, log_args=False, nout=None):
    """Decorator to define a task.

    Keyword arguments
    -----------------
    cache : bool
        Cache the task result (default: True)
    log_args : bool
        Log arguments the task is called with (default: False)
    nout : int
        Number of return values of the wrapped function. Must be specified if
        more than 1.

    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wrapped = _log_call(func, log_args)
            if cache:
                wrapped = delayed(memory.cache(wrapped),
                                  nout=nout)(*args, **kwargs)
            else:
                wrapped = delayed(wrapped, nout=nout)(*args, **kwargs)
            return wrapped
        return wrapper
    return decorator


def make_task(func, *args, **kwargs):
    """Wrap a function in a task.

    Parameters
    ----------
    func : callable
        Function to wrap
    args
        Arguments for the function
    kwargs
        Keyword arugments for the function plus keyword arguments accepted by
        the :func:`task` decorator.

    """
    try:
        cache = kwargs.pop('cache')
    except KeyError:
        cache = True

    try:
        log_args = kwargs.pop('log_args')
    except KeyError:
        log_args = False

    @task(cache, log_args)
    @functools.wraps(func)
    def wrapped(*a, **k):
        return func(*a, **k)

    return wrapped(*args, **kwargs)