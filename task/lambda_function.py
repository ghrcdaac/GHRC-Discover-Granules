import os
import sys
from main import DiscoverGranules, memory_check

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    dg = DiscoverGranules(event)
    print(f'Pre discover memory: {memory_check()}')

    granule_dict = dg.discover_granules()
    print(f'Post discover memory: {memory_check()}')

    # ret_dict = dg.check_granule_updates(granule_dict)
    dg.check_granule_updates(granule_dict)
    print(f'Post update memory: {memory_check()}')

    # dg.check_granule_updates(granule_dict)
    # print(f'Post update memory: {memory_check()}')

    test = dg.cumulus_output_generator(granule_dict)
    print(f'Post generator call memory: {memory_check()}')
    del granule_dict
    print(f'Post delete call memory: {memory_check()}')

    test2 = {'granules': test.__next__()}
    print(f'Post test2 call memory: {memory_check()}')
    # print(test2)
    # return test2
    return {'granules': []}
    # return dg.generate_cumulus_output(granule_dict)
    # return dg.generate_cumulus_output(ret_dict)


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
