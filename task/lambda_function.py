import os

from main import DiscoverGranules


def lambda_handler(event, context=None):
    print(f"Event[{event}]")
    dg = DiscoverGranules()
    s3_key = f"{os.getenv('s3_key_prefix').rstrip('/')}/{dg.csv_file_name}"
    bucket_name = os.getenv("bucket_name")
    path, depth, file_reg_ex, dir_reg_ex = [event.get(ele) for ele in
                                            ['url_path', 'depth', 'file_reg_ex', 'dir_reg_ex']]
    path = "http://data.remss.com/ssmi/f16/bmaps_v07/y2020/m09/"
    s3_key = "some_key"
    bucket_name = "mlhsbx-public"
    depth = "2"
    if path:
        file_list = dg.get_files_link_http(s3_key=s3_key, bucket_name=bucket_name, url_path=path,
                                           file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex, depth=depth)
    else:
        # if no url path is provided it is assumed to be an update request
        file_list = dg.check_for_updates(s3_key=s3_key, bucket_name=bucket_name)

    dg.upload_to_s3(s3_key=s3_key, bucket_name=bucket_name, granule_list=file_list)
    return file_list
