import gc
import os
import sys
from main import DiscoverGranules, memory_check

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    # dg = DiscoverGranules(event)
    # print(f'Pre discover memory: {memory_check()}')
    #
    # granule_dict = dg.discover_granules()
    # print(f'Post discover memory: {memory_check()}')
    #
    # dg.check_granule_updates(granule_dict)
    # print(f'Post update memory: {memory_check()}')
    #
    # return {'granules': dg.cumulus_output_generator(granule_dict).__next__()}
    return DiscoverGranules(event).discover_test()


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
