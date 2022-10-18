import os

import psutil

from task.dgm import safe_call, Granule, initialize_db
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
    if dg_client.input == 3:
        dg_client.clean_database()
    else:
        if dg_client.discover_tf.get('discovered_granules_count', 0) == 0:
            discovered_granules_count = dg_client.discover_granules()
        else:
            discovered_granules_count = dg_client.discover_tf.get('discovered_granules_count', 0)

        # Fetch a batch of granules from the database sorted by
        with initialize_db(dg_client.db_file_path):
            batch = getattr(Granule, f'fetch_batch')(Granule(), collection_id=dg_client.collection_id, batch_size=dg_client.discover_tf.get('batch_size_max'), **{'logger': rdg_logger})
        rdg_logger.error(f'batch_size: {len(batch)}')
        batch_dict = {}
        for granule in batch:
            dg_client.populate_dict(batch_dict, granule.name, granule.etag, granule.granule_id, granule.collection_id,
                                    granule.last_modified, granule.size)
        output = dg_client.generate_lambda_output(batch_dict)

        # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules
        # step will be able to copy them. As of 06-17-2022 Cumulus sync granules does not support access keys.
        # Additionally the provider needs to be updated to use the new location.
        # if dg_client.meta.get('aws_key_id_name', None) and dg_client.meta.get('aws_secret_key_name', None):
        #     dg_client.move_granule_wrapper(granule_dict)
        #     dg_client.provider['id'] = 'private_bucket'
        #     dg_client.provider['host'] = f'{os.getenv("stackName")}-private'
    qgc = int(dg_client.discover_tf.get('queued_granules_count', 0)) + len(output)
    a = {'granules': output, 'batch_size': len(output), 'discovered_granules_count': discovered_granules_count, 'queued_granules_count': qgc}
    rdg_logger.info(f'returning: {a}')
    return a


if __name__ == '__main__':
    pass
