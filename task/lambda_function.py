import os
import sys

from task.logger import rdg_logger
from task.main import main


if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """
    AWS Lambda handler
    :param event: Cumulus workflow event used to setup the discover granules class
    :param context: Unused variable but is required as the rum_cumulus_task passes it when the lambda handler is called
    :return: Formatted output of the event with any discovered granules in the payload
    """
    return main(event)


def handler(event, context):
    rdg_logger.info(f'Full Event: {event}')
    result = []
    if run_cumulus_task:
        result = run_cumulus_task(lambda_handler, event, context)
        rdg_logger.info(f'result: {result}')
    return result
