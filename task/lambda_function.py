from datetime import time
from time import mktime, strptime

from main import DiscoverGranules


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    collection = event['meta']['collection']
    provider = event['meta']['provider']
    discover_tf = collection['meta']['discover_tf']

    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{collection['meta']['provider_path'].lstrip('/')}"

    granule_dict = dg.get_file_links_http(url_path=path, file_reg_ex=discover_tf['file_reg_ex'],
                                          dir_reg_ex=discover_tf['dir_reg_ex'], depth=discover_tf['depth'])
    ret_dict = dg.check_granule_updates(granule_dict)

    granules = []
    for key, value in ret_dict.items():
        time_str = f"{value['date_modified']} {value['time_modified']} {value['meridiem_modified']}"
        p = '%m/%d/%Y %I:%M %p'
        epoch = int(mktime(strptime(time_str, p)))
        granules.append(
            {
                "granule_id": key,
                "data_type": value["filename"],
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
        )

    event["payload"] = granules
    return event
