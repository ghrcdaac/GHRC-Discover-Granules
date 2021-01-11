from main import DiscoverGranules

print('Loading function')


def lambda_handler(event, context=None):
    path = event['url_path']
    depth = int(event['depth'])
    file_reg_ex = event['file_reg_ex']
    dir_reg_ex = event['dir_reg_ex']

    return DiscoverGranules.get_files_link_http(url_path=path, file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex,
                                                depth=depth)
