import os
import sys


from task.main import DiscoverGranules


if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task

    # def temp():
    #     t = f'/mnt/data2esf/discover_granules.lock'
    #     a = open(t, 'w+')
    #     b = open(t, 'w+')
    #     fcntl.fcntl(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #     fcntl.fcntl(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #     fcntl.fcntl(b, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # fcntl.lockf(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # fcntl.lockf(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # fcntl.lockf(b, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # while True:
        #     pass
        # a.close()
        # b.close()

        # a = open(t, 'w+')
        # b = open(t, 'w+')
        # fcntl.flock(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # fcntl.flock(a, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # a.close()
        # fcntl.flock(b, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # b.close()


def lambda_handler(event, context=None):
    # print('Does anyone even work here anymore?')
    # try:
    #     temp()
    # except BlockingIOError:
    #     print('Caught the error')
    return DiscoverGranules(event).discover()


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
