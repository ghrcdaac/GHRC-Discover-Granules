import os

from main import DiscoverGranules


def lambda_handler(event, context=None):
    dg = DiscoverGranules()
    path = event['url_path']
    depth = int(event['depth'])
    file_reg_ex = event['file_reg_ex']
    dir_reg_ex = event['dir_reg_ex']
    # file_list = dg.get_files_link_http(url_path=path, file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex, depth=depth)
    s3_key = os.getenv("prefix") + "/" + dg.csv_file_name
    print("s3_key[" + s3_key + "]")
    print("bucket_name[" + os.getenv("bucket_name") + "]")
    file_list = dg.check_for_updates(s3_key=s3_key, bucket_name=os.getenv("bucket_name"))
    # dg.upload_file_mine(file_name=str(DiscoverGranules.csv_file), bucket_name=os.getenv("bucket_name"))
    # file_list = dg.download_file_mine(file_name=str(DiscoverGranules.csv_file), bucket_name=os.getenv("bucket_name"))
    return file_list
