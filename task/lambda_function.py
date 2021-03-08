import logging
import os


from botocore.exceptions import ClientError

from main import DiscoverGranules

print('Loading function')


def lambda_handler(event, context=None):
    file_list = []
    path = event['url_path']
    depth = int(event['depth'])
    file_reg_ex = event['file_reg_ex']
    dir_reg_ex = event['dir_reg_ex']
    # file_list = DiscoverGranules.get_files_link_http(url_path=path, file_reg_ex=file_reg_ex, dir_reg_ex=dir_reg_ex,
    #                                                  depth=depth)
    s3_key = os.getenv("prefix") + "/" + DiscoverGranules.csv_file
    print("s3_key[" + s3_key + "]")
    print("bucket_name[" + os.getenv("bucket_name") + "]")
    file_list = DiscoverGranules.check_for_updates(file_name=s3_key, bucket_name=os.getenv("bucket_name"))
    # DiscoverGranules.upload_file_mine(file_name=str(DiscoverGranules.csv_file), bucket_name=os.getenv("bucket_name"))
    # file_list = DiscoverGranules.download_file_mine(file_name=str(DiscoverGranules.csv_file), bucket_name=os.getenv("bucket_name"))
    return file_list
