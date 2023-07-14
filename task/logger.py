import logging
import os
from cumulus_logger import CumulusLogger


class LocalLogger:
    """
    Class used for logging if the CumulusLogger is not available
    """
    log = print
    trace = print
    debug = print
    info = print
    warn = print
    warning = print
    error = print


gdg_logger = CumulusLogger(name='GHRC-Discover-Granules', level=logging.INFO) \
    if os.getenv('enable_logging', 'false').lower() == 'true' else LocalLogger()

if __name__ == '__main__':
    pass
