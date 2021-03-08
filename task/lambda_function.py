import os

from main import DiscoverGranules


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    s3_key = os.getenv("prefix") + "/" + dg.csv_file_name
    bucket_name = os.getenv("bucket_name")
    if 'url_path' in event:
        path = event['url_path']
        if 'depth' in event:
            depth = int(event['depth'])
        if 'file_reg_ex' in event:
            file_reg_ex = event['file_reg_ex']
        if 'dir_reg_ex' in event:
            dir_reg_ex = event['dir_reg_ex']
        file_list = dg.get_files_link_http(url_path=path, file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex, depth=depth)
    else:
        # if no url path is provided it is assumed to be an update request
        file_list = dg.check_for_updates(s3_key=s3_key, bucket_name=bucket_name)

    dg.upload_to_s3(s3_key=s3_key, bucket_name=bucket_name, granule_list=file_list)
    return file_list
