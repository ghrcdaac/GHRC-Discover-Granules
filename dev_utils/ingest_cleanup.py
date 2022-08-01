import concurrent.futures
import boto3


"""
This script is only intended to be used by developers as a way to quickly cleanup ingest workflow executions. The ARN
is hardcoded but should only ever have to be updated if the SBX stack gets redeployed. 
"""
boto3.setup_default_session(profile_name='<ADD_PROFILE_HERE>')


def main():
    client = boto3.client('stepfunctions')

    tasks_to_kill = []
    args = {
        'stateMachineArn': 'arn:aws:states:us-west-2:<ADD_ACCOUNT_NUMBER_HERE>:stateMachine:<ADD_STACK_PREFIX_HERE>-IngestGranule',
        'statusFilter': 'RUNNING'
    }

    while True:
        while True:
            resp = client.list_executions(**args)
            tasks_to_kill += resp.get("executions")
            next_token = resp.get('nextToken')
            if next_token:
                args.setdefault('nextToken', next_token)
            else:
                break

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for e in resp.get('executions'):
                futures.append(
                    executor.submit(client.stop_execution, executionArn=e.get('executionArn'))
                )

            for future in concurrent.futures.as_completed(futures):
                error = future.exception()
                if error:
                    print(f'Error: {error}')
                else:
                    print(future.result())

        print(f'Killed {len(tasks_to_kill)} tasks.')


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
    main()
