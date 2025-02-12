import json
import os

from task.discover_granules_ftp import DiscoverGranulesFTP
from task.discover_granules_http import DiscoverGranulesHTTP
from task.discover_granules_s3 import DiscoverGranulesS3
from task.discover_granules_sftp import DiscoverGranulesSFTP
from task.logger import gdg_logger


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
        'sftp': DiscoverGranulesSFTP,
        'ftp': DiscoverGranulesFTP
    }
    try:
        gdg_logger.info(f'trying protocol: {protocol}')
        dg_class = protocol_switch[protocol]
    except Exception as e:
        raise Exception(f"Protocol {protocol} is not supported: {str(e)}")

    return dg_class

def write_results_to_local_store(local_store, collection, res):
    print(f'is {local_store} a directory: {os.path.isdir(local_store)}')
    c_id = f'{collection.get("name")}__{collection.get("version")}'
    filename = f'{local_store}/{c_id}/{c_id}.json'
    print(f'Creating file: {filename}')
    with open(filename, 'w+') as test_file:
        test_file.write(json.dumps(res))
        print(f'Result written for granules: {len(res.get("granules", []))}')

    pass

def main(event, context):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    # print('BEGIN')
    # print(f'Event: {event}')
    # gdg_logger.info(f'Event: {event}')
    protocol = event['config']['provider']["protocol"].lower()
    dg_client = get_discovery_class(protocol)(event, context)
    if dg_client.discovered_files_count == 0 or dg_client.bookmark:
        res = dg_client.discover_granules()
    else:
        res = dg_client.read_batch()

    if not res.get('bookmark', None):
        granule_list_dicts = res.pop('batch')
        res.update({'batch_size': len(granule_list_dicts)})

        # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules
        # step will be able to copy them. As of 06-17-2022 Cumulus sync granules does not support access keys.
        # Additionally the provider needs to be updated to use the new location.
        if dg_client.meta.get('aws_key_id_name', None) and dg_client.meta.get('aws_secret_key_name', None):
            gdg_logger.info('Granules are in an external provider. Updating output to internal bucket.')
            dg_client.move_granule_wrapper(granule_list_dicts)
            external_host = dg_client.provider.get('external_host', None)
            gdg_logger.info(f'external_host was: {external_host}')
            if not external_host:
                dg_client.provider.update({'external_id': dg_client.provider.get('id')})
                dg_client.provider.update({'external_host': dg_client.provider.get('host')})
                gdg_logger.info(f'updated_provider: {dg_client.provider}')
            dg_client.provider['id'] = 'private_bucket'
            dg_client.provider['host'] = f'{os.getenv("stackName")}-private'

            # Update the granule name before producing the cumulus output
            gdg_logger.info(f'Updating external S3 URIs...')
            for granule_dict in granule_list_dicts:
                granule_name = granule_dict.get('name')
                path = granule_name.replace('s3://', '').split('/', maxsplit=1)[-1]
                new_uri = f's3://{dg_client.provider.get("host")}/{path}'
                granule_dict.update({'name': new_uri})

        cumulus_output = dg_client.generate_lambda_output(granule_list_dicts)
        res.update(
            {
                'granules': cumulus_output,
                'queued_granules_count': int(dg_client.discover_tf.get('queued_granules_count', 0)) + len(cumulus_output)
            }
        )
    else:
        res.update(
            {
                'granules': [],
                'queued_granules_count': 0
            }
        )
    
    local_store = event.get('shared_store', os.getenv('EBS_MNT'))
    if local_store:
        write_results_to_local_store(local_store, dg_client.collection, res)
        
    return res


if __name__ == '__main__':
    pass
