import os

from task.discover_granules_ftp import DiscoverGranulesFTP
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
        'sftp': DiscoverGranulesSFTP,
        'ftp': DiscoverGranulesFTP
    }
    try:
        rdg_logger.info(f'trying protocol: {protocol}')
        dg_class = protocol_switch[protocol]
    except Exception as e:
        raise Exception(f"Protocol {protocol} is not supported: {str(e)}")

    return dg_class


def main(event, context):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    # rdg_logger.info(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol").lower()
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
            rdg_logger.info('Granules are in an external provider. Updating output to internal bucket.')
            dg_client.move_granule_wrapper(granule_list_dicts)
            external_host = dg_client.provider.get('external_host', None)
            rdg_logger.info(f'external_host was: {external_host}')
            if not external_host:
                dg_client.provider.update({'external_id': dg_client.provider.get('id')})
                dg_client.provider.update({'external_host': dg_client.provider.get('host')})
                rdg_logger.info(f'updated_provider: {dg_client.provider}')
            dg_client.provider['id'] = 'private_bucket'
            dg_client.provider['host'] = f'{os.getenv("stackName")}-private'

            # Update the granule name before producing the cumulus output
            rdg_logger.info(f'Updating external S3 URIs...')
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
    # rdg_logger.info(f'returning: {res}')
    return res


if __name__ == '__main__':
    pass
