# -*- coding: utf-8 -*-
import logging
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())

log.setup_logging()

__version__ = '1.0.0'


