import os
import sys

from task.test import test_main
from task.logger import rdg_logger
from task.main import main

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def handler(event, context):
    rdg_logger.info(f'Full Event: {event}')
    if event.get('is_test', False):
        results = test_main(event, context)
    else:
        if run_cumulus_task:
            results = run_cumulus_task(main, event, context)
            rdg_logger.info(f'result: {results}')
        else:
            results = main(event, context)
    return results
