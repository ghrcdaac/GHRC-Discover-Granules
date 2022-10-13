import os

import psutil

from task.discover_granules_http import DiscoverGranulesHTTP
from task.discover_granules_s3 import DiscoverGranulesS3
from task.discover_granules_sftp import DiscoverGranulesSFTP
from task.logger import rdg_logger


def get_discovery_class(protocol):
    """
    Takes in a string parameter and attempts to return the class for a particular protocol.
    :param protocol: The protocol that granules need to be discovered on.
    :return A discover granules class for the appropriate protocol
    """
    protocol_switch = {
        'http': DiscoverGranulesHTTP,
        'https': DiscoverGranulesHTTP,
        's3': DiscoverGranulesS3,
        'sftp': DiscoverGranulesSFTP
    }
    try:
        dg_class = protocol_switch[protocol]
    except Exception as e:
        raise Exception(f"Protocol {protocol} is not supported: {str(e)}")

    return dg_class


def main(event):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    rdg_logger.info(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol").lower()
    dg_client = get_discovery_class(protocol)(event)
    output = {}
    if dg_client.input:
        dg_client.clean_database()
    elif dg_client.discover_tf.get('batch_request'):
        # Fetch a batch of granules from the database sorted by
        pass
    else:
        dg_dict = dg_client.discover_granules()
        rdg_logger.info(f'Discovered {len(dg_dict)} granules.')
        dg_client.check_granule_updates_db(dg_dict)
        output = dg_client.generate_lambda_output(dg_dict)
        rdg_logger.info(f'Returning cumulus output for {len(output)} {dg_client.collection.get("name")} granules.')

        # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules
        # step will be able to copy them. As of 06-17-2022 Cumulus sync granules does not support access keys.
        # Additionally the provider needs to be updated to use the new location.
        if dg_client.meta.get('aws_key_id_name', None) and dg_client.meta.get('aws_secret_key_name', None):
            dg_client.move_granule_wrapper(dg_dict)
            dg_client.provider['id'] = 'private_bucket'
            dg_client.provider['host'] = f'{os.getenv("stackName")}-private'

    a = {'granules': output, 'batch_size': len(output)}
    rdg_logger.info(f'returning: {a}')

    return {
        'granules': output,
        'batch_size': len(output),
        'queued_granules_count': len(output),
        'discovered_granules_count': len(output)
    }


def test():
    t = {}
    for x in range(5000000):
        t[f'a_{x}'] = {'value': x, 'and_something_else': x}

    process = psutil.Process(os.getpid())
    print(process.memory_info().rss / 1024 ** 2)  # in bytes
    print(len(t))
    return t


if __name__ == '__main__':
    test()
    pass
