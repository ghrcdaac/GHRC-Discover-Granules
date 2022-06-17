import logging
import os

from task.dgm import initialize_db, Granule
from task.discover_granules_http import DiscoverGranulesHTTP
from task.discover_granules_s3 import DiscoverGranulesS3
from task.discover_granules_sftp import DiscoverGranulesSFTP
from task.helpers import MyLogger
from cumulus_logger import CumulusLogger

rdg_logger = CumulusLogger(name='Recursive-Discover-Granules', level=logging.INFO) \
    if os.getenv('enable_logging', 'false').lower() == 'true' else MyLogger()


def get_discovery_class(protocol):
    """
    Takes in a string parameter and attempts to return the class for a particular protocol.
    :param protocol: The protocol that granules need to be discovered on.
    :return A discover granules class for the appropriate protocol
    """
    switcher = {
        'http': DiscoverGranulesHTTP,
        'https': DiscoverGranulesHTTP,
        's3': DiscoverGranulesS3,
        'sftp': DiscoverGranulesSFTP
    }

    return switcher.get(protocol)


def discover_granules(event):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    rdg_logger.warning(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol")
    try:
        dg = get_discovery_class(protocol)(event, rdg_logger)
    except Exception:
        raise Exception(f"Protocol {protocol} is not supported")

    output = {}
    if dg.input:
        clean_database(dg)
    else:
        output = discovery(dg)

    dg.logger.info(f'Discovered {len(output)} granules.')

    if os.getenv('no_return', 'false').lower() == 'true':
        dg.logger.warning('no_return is set to true. No output will be returned.')
        output = []

    return {'granules': output}


def clean_database(dg):
    """
    If there is input in the event then QueueGranules failed and we need to clean out the discovered granules
    from the database.
    :param dg Initialized discover granules object.
    """
    names = []
    dg.logger.warning(dg.input.get('granules', {}))
    for granule in dg.input.get('granules', {}):
        file = granule.get('files')[0]
        name = f'{file.get("path")}/{file.get("name")}'
        names.append(name)

    with initialize_db(dg.db_file_path):
        num = Granule().delete_granules_by_names(names)

    dg.logger.info(f'Cleaned {num} records from the database.')


def discovery(dg):
    """
    Discovers granules, checks against the database, and returns correctly formatted output.
    :param dg: Initialized discover granules object
    :return: List of formatted dictionaries.
    """
    granule_dict = dg.discover_granules()
    if not granule_dict:
        dg.logger.warning(f'Warning: Found 0 {dg.collection.get("name")} granules at the provided location.')
    else:
        dg.logger.info(f'Discovered {len(granule_dict)} {dg.collection.get("name")} '
                       f'granules for update processing.')
    dg.check_granule_updates_db(granule_dict)

    # If keys were provided then we need to relocate the granules to the GHRC private bucket so the sync granules step
    # will be able to copy them. As of 06-17-2022 Cumulus sync ganules does not support access keys. Additionally the
    # provider needs to be updated to use the new location.
    if dg.meta.get('aws_key_id_name', None) and dg.meta.get('aws_secret_key_name', None):
        dg.move_granule_wrapper(granule_dict)
        dg.provider['id'] = 'private_bucket'
        dg.provider['host'] = f'{os.getenv("stackName")}-private'

    output = dg.generate_lambda_output(granule_dict)
    dg.logger.info(f'Returning cumulus output for {len(output)} {dg.collection.get("name")} granules.')

    return output


if __name__ == '__main__':
    pass
