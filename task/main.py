import os

from task.dgm import safe_call, Granule
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


def main(event):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    rdg_logger.info(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol").lower()
    dg_client = get_discovery_class(protocol)(event)
    ret = None
    if dg_client.input == 3:
        rdg_logger.warning(f'The database is being cleaning and this should not happen right now.')
        dg_client.clean_database()
    else:
        # If discovered_granules_count is already in the event then this is an ongoing execution
        # otherwise check if there are already discovered granules that need to be queued`
        discovered_granules_count = dg_client.discover_tf.get('discovered_granules_count', None)
        if not discovered_granules_count:
            discovered_granules_count = safe_call(
                dg_client.db_file_path,
                getattr(Granule, 'count_discovered'),
                **{'collection_id': dg_client.collection_id}
            )
            if discovered_granules_count == 0 or (
                    dg_client.duplicates == 'replace' and dg_client.discover_tf.get('force_replace') == 'true'):
                discovered_granules_count = dg_client.discover_granules()

        rdg_logger.info('Fetching batch...')
        batch = safe_call(
            dg_client.db_file_path,
            getattr(Granule, 'fetch_batch'),
            **{
                'collection_id': dg_client.collection_id,
                'batch_size': dg_client.discover_tf.get('batch_limit'),
                'provider_path': dg_client.collection.get('meta').get('provider_path'),
                'logger': rdg_logger
               }
        )

        # Convert peewee model objects to a dictionary
        batch_dict = {}
        for granule in batch:
            dg_client.populate_dict(
                batch_dict, granule.name,
                granule.etag, granule.granule_id,
                granule.collection_id, granule.last_modified,
                granule.size
            )

        # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules
        # step will be able to copy them. As of 06-17-2022 Cumulus sync granules does not support access keys.
        # Additionally the provider needs to be updated to use the new location.
        if dg_client.meta.get('aws_key_id_name', None) and dg_client.meta.get('aws_secret_key_name', None):
            rdg_logger.info(f'Granules are in an external provider. Updating output to internal bucket.')
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
        qgc = int(dg_client.discover_tf.get('queued_granules_count', 0)) + len(cumulus_output)

        ret = {
            'granules': cumulus_output,
            'batch_size': len(cumulus_output),
            'discovered_granules_count': discovered_granules_count,
            'queued_granules_count': qgc
        }

    rdg_logger.info(f'returning: {ret}')
    return ret


if __name__ == '__main__':
    pass
