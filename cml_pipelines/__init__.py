from collections import namedtuple
import logging

from .cache import memory
from .pipeline import Pipeline
from .wrapper import task, make_task

__version__ = '1.1.1'
version_info = namedtuple("VersionInfo", "major,minor,patch")(*__version__.split('.'))

logger = logging.getLogger("cml.pipelines")
logger.addHandler(logging.NullHandler())
