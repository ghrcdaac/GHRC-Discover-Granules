import concurrent.futures
import os
from time import sleep

import boto3


"""
This script is only intended to be used by developers as a way to quickly cleanup ingest workflow executions. The ARN
is hardcoded but should only ever have to be updated if the SBX stack gets redeployed. 
"""


def kill_funct(executions, client):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for e in executions:
            print(f'Submitting for termination: {e.get("executionArn")}')
            futures.append(
                executor.submit(client.stop_execution, executionArn=e.get('executionArn'))
            )

        for future in concurrent.futures.as_completed(futures):
            error = future.exception()
            if error:
                print(f'Error: {error}')


def cleanup():
    boto3.setup_default_session(profile_name=os.getenv('AWS_PROFILE'), region_name=os.getenv('AWS_REGION'))
    client = boto3.client('stepfunctions')

    args = {
        'stateMachineArn': f'arn:aws:states:us-west-2:{os.getenv("ACCOUNT_NUMBER")}'
                           f':stateMachine:{os.getenv("STACK_PREFIX")}-IngestGranule',
        'statusFilter': 'RUNNING'
    }

    while True:
        while True:
            resp = client.list_executions(**args)
            tasks_to_kill = resp.get("executions")
            kill_funct(tasks_to_kill, client)
            next_token = resp.get('nextToken')
            if next_token:
                args.setdefault('nextToken', next_token)
            else:
                break

        print('waiting for tasks...')
        sleep(5)


if __name__ == '__main__':
    cleanup()
