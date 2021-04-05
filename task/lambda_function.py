import os
import sys
from time import mktime, strptime
from main import DiscoverGranules

run_cumulus_task = None
if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    config = event['config']
    provider = config['provider']
    collection = event['config']['collection']
    discover_tf = collection['meta']['discover_tf']

    # event['config']['collection']['meta']['discover_tf']['output']
    print(f"discover_tf[{str(discover_tf)}]")

    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{config['provider_path'].lstrip('/')}"
    print(f"path[{path}]")

    granule_dict = dg.get_file_links_http(url_path=path, file_reg_ex=discover_tf['file_reg_ex'],
                                          dir_reg_ex=discover_tf['dir_reg_ex'], depth=discover_tf['depth'])
    ret_dict = dg.check_granule_updates(granule_dict)

    discovered_granules = {}
    for key, value in ret_dict.items():
        time_str = f"{value['date_modified']} {value['time_modified']} {value['meridiem_modified']}"
        p = '%m/%d/%Y %I:%M %p'
        epoch = int(mktime(strptime(time_str, p)))
        discovered_granules[key] = {
                "granule_id": value["filename"],
                "data_type": "",
                "version": "1",
                "files": [
                    {
                        "name": value["filename"],
                        "path": key,
                        "size": "",
                        "time": epoch,
                        "bucket": dg.s3_bucket_name,
                        "url_path": "",
                        "type": ""
                    }
                ]
            }

    return discovered_granules


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
