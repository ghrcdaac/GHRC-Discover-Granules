import os
import sys
from time import mktime, strptime
from main import DiscoverGranules

run_cumulus_task = None
if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    config = event['config']
    provider = config['provider']
    collection = event['config']['collection']
    discover_tf = collection['meta']['discover_tf']
    file_reg_ex = collection.get('granuleIdExtraction')
    csv_file_name = f"{collection['name']}__{collection['version']}.csv"
    dg = DiscoverGranules(csv_file_name=csv_file_name)
    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{config['provider_path'].lstrip('/')}"
    granule_dict = dg.get_file_links_http(url_path=path, file_reg_ex=file_reg_ex,
                                          dir_reg_ex=discover_tf['dir_reg_ex'], depth=discover_tf['depth'])
    ret_dict = dg.check_granule_updates(granule_dict)
    discovered_granules = []
    p = '%a%d%b%Y%H:%M:%S%Z'
    for key, value in ret_dict.items():
        epoch = int(mktime(strptime(value['Last-Modified'].replace(' ', ''), p)))
        host = config['provider']["host"]
        filename = key.rsplit('/')[-1]
        path = key[key.find(host) + len(host): key.find(filename)]
        discovered_granules.append({
                "granuleId": filename,
                "dataType": collection.get("name", ""),
                "version": collection.get("version", ""),
                "files": [
                    {
                        "name": filename,
                        "path": path,
                        "size": "",
                        "time": epoch,
                        "bucket": dg.s3_bucket_name,
                        "url_path": collection.get("url_path", ""),
                        "type": ""
                    }
                ]
            })

    return {"granules": discovered_granules}


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
