import concurrent.futures
import os
from time import sleep

import boto3


"""
This script is only intended to be used by developers as a way to quickly cleanup ingest workflow executions. The ARN
is hardcoded but should only ever have to be updated if the SBX stack gets redeployed. 
"""


def cleanup():
    boto3.setup_default_session(profile_name=os.getenv('AWS_PROFILE'), region_name=os.getenv('AWS_REGION'))
    client = boto3.client('stepfunctions')

    tasks_to_kill = []
    tasks_to_kill_len = len(tasks_to_kill)
    args = {
        'stateMachineArn': f'arn:aws:states:us-west-2:{os.getenv("ACCOUNT_NUMBER")}'
                           f':stateMachine:{os.getenv("STACK_PREFIX")}-IngestGranule',
        'statusFilter': 'RUNNING'
    }

    while True:
        while True:
            resp = client.list_executions(**args)
            tasks_to_kill += resp.get("executions")
            next_token = resp.get('nextToken')
            if next_token and not len(tasks_to_kill) % 100:
                args.setdefault('nextToken', next_token)
            else:
                break

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for e in resp.get('executions'):
                print(f'Submitting for termination: {e.get("executionArn")}')
                futures.append(
                    executor.submit(client.stop_execution, executionArn=e.get('executionArn'))
                )

            for future in concurrent.futures.as_completed(futures):
                error = future.exception()
                if error:
                    print(f'Error: {error}')

        if len(tasks_to_kill) != tasks_to_kill_len:
            tasks_to_kill_len = len(tasks_to_kill)
            print(f'Killed {len(tasks_to_kill)} tasks.')
        else:
            print('waiting for tasks...')
            sleep(5)


def fast_copy():
    # Create file test_0.txt
    client = boto3.client('s3')
    val = 0
    base_name = 'test_'
    ftype = '.txt'
    # Loop for 10000
    with open(f'{base_name}{val}{ftype}'):
        pass

    for val in range(30000):

        # Upload file
        client.put_object(
            Bucket='sharedsbx-private',
            Body=open(f'{base_name}{val}{ftype}'),
            Key='fake_collection_mlh/dir1/'

        )

        # Rename file


if __name__ == '__main__':
    pass