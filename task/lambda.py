import os
import sys

from task.logger import gdg_logger
from task.main import main

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def handler(event, context):
    # gdg_logger.info(f'Full Event: {event}')
    if run_cumulus_task:
        results = run_cumulus_task(main, event, context)
        # gdg_logger.info(f'result: {results}')
    else:
        results = main(event, context)
    return results
