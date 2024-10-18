import json
import os
import signal
import subprocess
import sys
import time

from task.test import test_main
from task.logger import gdg_logger
from task.main import main

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def handler(event, context):
    # gdg_logger.info(f'Full Event: {event}')
    if event.get('is_test', False):
        results = test_main(event, context)
    else:
        if run_cumulus_task:
            results = run_cumulus_task(main, event, context)
            # gdg_logger.info(f'result: {results}')
        else:
            results = main(event, context)
    return results

def handler_act(event, context):
    print(f'GDG handler event: {event}')
    return main(event, context)


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print('Exiting gracefully')
        self.kill_now = True


if __name__ == '__main__':
    # print(f'GDG argv: {sys.argv}')
    if len(sys.argv) <= 1:
        killer = GracefulKiller()
        print('GDG Task is running...')
        while not killer.kill_now:
            time.sleep(1)
        print('GDG terminating')
    else:
        print('GDG calling function')
        # print(f'argv: {type(sys.argv[1])}')
        # print(f'argv: {sys.argv[1]}')
        handler_act(json.loads(sys.argv[1]), {})


