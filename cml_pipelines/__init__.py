from collections import namedtuple
import logging

from .pipeline import Pipeline

__version__ = "2.0.0"
version_info = namedtuple("VersionInfo", "major,minor,patch")(*__version__.split('.'))

logger = logging.getLogger("cml.pipelines")
logger.addHandler(logging.NullHandler())
