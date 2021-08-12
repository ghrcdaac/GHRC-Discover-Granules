import os
import sys
from main import DiscoverGranules

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    dg = DiscoverGranules(event)
    granule_dict = dg.discover_granules()
    ret_dict = dg.check_granule_updates(granule_dict)
    return dg.generate_cumulus_output(ret_dict)


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
