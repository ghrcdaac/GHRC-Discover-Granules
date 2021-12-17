import concurrent.futures
import boto3

if __name__ == '__main__':
    client = boto3.client('stepfunctions')

    tasks_to_kill = []
    args = {
        'stateMachineArn': 'arn:aws:states:us-west-2:322322076095:stateMachine:sharedsbx-IngestGranule',
        'statusFilter': 'RUNNING'
    }

    while True:
        resp = client.list_executions(**args)
        tasks_to_kill += resp.get("executions")
        nextToken = resp.get('nextToken')
        if nextToken:
            args.setdefault('nextToken', nextToken)
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
