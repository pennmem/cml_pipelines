from collections import namedtuple
import logging

from .wrapper import task, make_task

__version__ = '1.1.0'
version_info = namedtuple("VersionInfo", "major,minor,patch")(*__version__.split('.'))

logger = logging.getLogger("cml.pipelines")
logger.addHandler(logging.NullHandler())
