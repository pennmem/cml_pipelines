from collections import namedtuple
import logging

from .pipeline import Pipeline

__version__ = '1.1.1'
version_info = namedtuple("VersionInfo", "major,minor,patch")(*__version__.split('.'))

logger = logging.getLogger("cml.pipelines")
logger.addHandler(logging.NullHandler())
