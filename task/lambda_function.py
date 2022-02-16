import os
import sys

sys.path.insert(0, f'{os.environ.get("efs_mount")}/dependencies/')
from task.main import DiscoverGranules


if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    return DiscoverGranules(event).discover()


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []


if __name__ == '__main__':
    pass
