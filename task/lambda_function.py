import os
import sys

from task.main import discover_granules

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    return discover_granules(event)


def handler(event, context):
    result = []
    if run_cumulus_task:
        result = run_cumulus_task(lambda_handler, event, context)
    return result
