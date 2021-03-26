from main import DiscoverGranules


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    collection = event['meta']['collection']
    provider = event['meta']['provider']
    discover_tf = collection['meta']['discover_tf']

    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{collection['meta']['provider_path'].lstrip('/')}"

    granule_dict = dg.get_files_link_http(url_path=path, file_reg_ex=discover_tf['file_reg_ex'],
                                          dir_reg_ex=discover_tf['dir_reg_ex'], depth=discover_tf['depth'])

    # Return a list formatted appropriately
    ret_list = []
    for key, value in granule_dict.items():
        temp_dict = {
            'granuleId': value['filename'],
            'dataType': 'dataType',
            'version': collection['version'],
            'files': [
                {
                    "name": value['filename'],
                    "path": key,
                    "bucket": dg.s3_bucket_name,
                    "url_path": collection['url_path'],
                    "type": ""
                }
            ]
        }
        ret_list.append(granule_dict)

    event["payload"] = [granule_dict]
    # dg.upload_to_s3(granule_list=granule_dict)
    return event
