import os

from main import DiscoverGranules


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    s3_key = f"{os.getenv('s3_key_prefix').rstrip('/')}/{dg.csv_file_name}"
    bucket_name = os.getenv("bucket_name")

    collection = event['meta']['collection']
    provider = event['meta']['provider']
    discover_tf = collection['meta']['discover_tf']

    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{collection['meta']['provider_path'].lstrip('/')}"

    if path:
        file_list = dg.get_files_link_http(s3_key=s3_key, bucket_name=bucket_name, url_path=path,
                                           file_reg_ex=discover_tf['file_reg_ex'],
                                           dir_reg_ex=discover_tf['dir_reg_ex'],
                                           depth=discover_tf['depth'])
    else:
        # if no url path is provided it is assumed to be an update request
        file_list = dg.check_for_updates(s3_key=s3_key, bucket_name=bucket_name)

    # Return a list formatted appropriately
    ret_list = []
    for granule in file_list:
        temp_dict = {
            'granuleId': granule.filename,
            'dataType': 'dataType',
            'version': collection['version'],
            'files': [
                {
                    "name": granule.filename,
                    "path": granule.link,
                    "bucket": bucket_name,
                    "url_path": collection['url_path'],
                    "type": ""
                }
            ]
        }
        ret_list.append(temp_dict)

    event["payload"] = [ret_list]
    dg.upload_to_s3(s3_key=s3_key, bucket_name=bucket_name, granule_list=file_list)
    return event
