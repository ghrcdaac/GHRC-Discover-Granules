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

    event["payload"] = [ret_dict]
    return event
