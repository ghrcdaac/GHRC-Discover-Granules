import os

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
    rdg_logger.info(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol").lower()
    dg_client = get_discovery_class(protocol)(event)
    res = dg_client.discover_granules()

    batch_dict = res.pop('batch')
    res.update({'batch_size': len(batch_dict)})

    # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules
    # step will be able to copy them. As of 06-17-2022 Cumulus sync granules does not support access keys.
    # Additionally the provider needs to be updated to use the new location.
    if dg_client.meta.get('aws_key_id_name', None) and dg_client.meta.get('aws_secret_key_name', None):
        rdg_logger.info('Granules are in an external provider. Updating output to internal bucket.')
        dg_client.move_granule_wrapper(batch_dict)
        dg_client.provider['id'] = 'private_bucket'
        dg_client.provider['host'] = f'{os.getenv("stackName")}-private'

        # Update the granule name before producing the cumulus output
        for granule_name in list(batch_dict.keys()):
            path = granule_name.replace('s3://', '').split('/', maxsplit=1)[-1]
            new_uri = f's3://{dg_client.provider["host"]}/{path}'
            batch_dict[new_uri] = batch_dict[granule_name]
            del batch_dict[granule_name]

    cumulus_output = dg_client.generate_lambda_output(batch_dict)
    res.update(
        {
            'granules': cumulus_output,
            'queued_granules_count': int(dg_client.discover_tf.get('queued_granules_count', 0)) + len(cumulus_output)
        }
    )

    rdg_logger.info(f'returning: {res}')
    return res


if __name__ == '__main__':
    pass
