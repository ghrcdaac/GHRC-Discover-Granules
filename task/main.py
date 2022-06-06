import logging
import os

import boto3

from task.dgm import Granule
from task.discover_granules_http import DiscoverGranulesHTTP
from task.discover_granules_s3 import DiscoverGranulesS3
from task.discover_granules_sftp import DiscoverGranulesSFTP
from cumulus_logger import CumulusLogger
from task.helpers import MyLogger

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

    try:
        return switcher[str(protocol).lower()]
    except KeyError:
        raise ValueError(f'Protocol {protocol} is not supported.')


def test_funct(event):
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(
        SecretId=os.environ.get('RDS_CREDENTIALS_SECRET_ARN')
    )
    # for key in resp:
    rdg_logger.warning(f'key: {resp.get("SecretString")}')

    return event


def discover_granules(event):
    """
    Function to be called to trigger the granule discover process once the class has been initialized with the
    correct cumulus event
    """
    rdg_logger.warning(f'Event: {event}')
    protocol = event.get('config').get('provider').get("protocol")
    discover = get_discovery_class(protocol)(event, rdg_logger)

    output = {}
    if discover.input:
        # If there is input in the event then QueueGranules failed and we need to clean out the discovered granules
        # from the database.
        names = []
        discover.logger.warning(discover.input.get('granules', {}))
        for granule in discover.input.get('granules', {}):
            file = granule.get('files')[0]
            name = f'{file.get("path")}/{file.get("name")}'
            names.append(name)

        num = Granule().delete_granules_by_names(names)
        discover.logger.info(f'Cleaned {num} records from the database.')
    else:
        # Discover granules
        granule_dict = discover.discover_granules()
        if not granule_dict:
            discover.logger.warning(f'Warning: Found 0 {discover.collection.get("name")} granules at the provided location.')
        else:
            discover.logger.info(f'Discovered {len(granule_dict)} {discover.collection.get("name")} '
                           f'granules for update processing.')
        discover.check_granule_updates_db(granule_dict)

        output = discover.cumulus_output_generator(granule_dict)
        discover.logger.info(f'Returning cumulus output for {len(output)} {discover.collection.get("name")} granules.')

    discover.logger.info(f'Discovered {len(output)} granules.')

    if os.getenv('no_return', 'false').lower() == 'true':
        discover.logger.warning('no_return is set to true. No output will be returned.')
        output = []

    return {'granules': output}


if __name__ == '__main__':
    pass
