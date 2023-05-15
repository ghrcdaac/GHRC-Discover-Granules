import concurrent.futures
import os
import re

import boto3

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger


def get_ssm_value(id_name, ssm_client):
    """
    Retrieves and decrypts ssm value from aws
    :param id_name: The identifier of the managed secret
    :param ssm_client:Initialized boto3 ssm client
    :return: Decrypted ssm value
    """
    return ssm_client.get_parameter(Name=id_name, WithDecryption=True).get('Parameter').get('Value')


def get_s3_client(aws_key_id=None, aws_secret_key=None):
    """
    Create and return an S3 client
    :param aws_key_id: If an access key is defined it will be used in the client initialization
    :param aws_secret_key: If a secret key is defined it will be used in the client initialization
    :return: An initialize boto3 s3 client
    """
    return boto3.client(
        's3',
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret_key
    )


def get_s3_client_with_keys(key_id_name, secret_key_name):
    """
    Gets a boto3 s3 client using an aws key id and secret key if provided
    :param key_id_name: ID of the aws key
    :param secret_key_name: Name of the aws key
    """
    ssm_client = boto3.client('ssm')
    return get_s3_client(aws_key_id=get_ssm_value(key_id_name, ssm_client),
                         aws_secret_key=get_ssm_value(secret_key_name, ssm_client))


def get_s3_resp_iterator(host, prefix, s3_client, pagination_config=None):
    """
    Returns an s3 paginator.
    :param host: The bucket.
    :param prefix: The path for the s3 granules.
    :param s3_client: Initialized boto3 S3 client
    :param pagination_config: Configuration for s3 pagination
    """
    if pagination_config is None:
        pagination_config = {'page_size': 1000}

    s3_paginator = s3_client.get_paginator('list_objects_v2')
    return s3_paginator.paginate(
        Bucket=host,
        Prefix=prefix,
        PaginationConfig=pagination_config
    )


class DiscoverGranulesS3(DiscoverGranulesBase):
    """
    Class to discover granules from S3 provider
    """
    def __init__(self, event):
        super().__init__(event)
        self.key_id_name = self.meta.get('aws_key_id_name')
        self.secret_key_name = self.meta.get('aws_secret_key_name')
        self.prefix = str(self.collection['meta']['provider_path']).lstrip('/')

    def discover_granules(self):
        try:
            rdg_logger.info(f'Discovering in {self.provider_url}')
            s3_client = get_s3_client() if None in [self.key_id_name, self.secret_key_name] \
                else get_s3_client_with_keys(self.key_id_name, self.secret_key_name)
            self.discover(get_s3_resp_iterator(self.host, self.prefix, s3_client))
            self.dbm.flush_dict()
            batch = self.dbm.read_batch()
        except ValueError as e:
            rdg_logger.error(e)
            raise
        finally:
            self.dbm.close_db()

        ret = {
            'discovered_files_count': self.dbm.discovered_files_count + self.discovered_files_count,
            'queued_files_count': self.dbm.queued_files_count,
            'batch': batch
        }

        return ret

    def discover(self, response_iterator):
        """
        Fetch the link of the granules in the host url_path
        :return: Returns a dictionary containing the path, etag, and the last modified date of a granule
        granule_dict = {
           's3://bucket-name/path/to/granule/file.extension': {
              'ETag': '<S3-etag-place-holder>',
              'Last-Modified': '1645564956.0
           },
           ...
        }
        """
        for page in response_iterator:
            for s3_object in page.get('Contents', {}):
                key = f'{self.provider.get("protocol")}://{self.provider.get("host")}/{s3_object["Key"]}'
                sections = str(key).rsplit('/', 1)
                key_dir = sections[0]
                url_segment = sections[1]
                if check_reg_ex(self.file_reg_ex, url_segment) and check_reg_ex(self.dir_reg_ex, key_dir):
                    etag = s3_object['ETag'].strip('"')
                    last_modified = s3_object['LastModified'].timestamp()
                    size = int(s3_object['Size'])
                    # print(f'Found: {key}')
                    reg_res = re.search(self.granule_id_extraction, url_segment)
                    if reg_res:
                        granule_id = reg_res.group(1)
                        self.dbm.add_record(
                            name=key, granule_id=granule_id,
                            collection_id=self.collection_id, etag=etag,
                            last_modified=str(last_modified), size=size
                        )

                    else:
                        rdg_logger.warning(
                            f'The collection\'s granuleIdExtraction {self.granule_id_extraction}'
                            f' did not match the filename {url_segment}.'
                        )

    def move_granule(self, source_s3_uri, destination_bucket=None):
        """
        Moves a granule from an external provider bucket to the ec2 mount location so that it can be uploaded to an
        internal S3 bucket.
        :param source_s3_uri: The external location to copy from
        :param destination_bucket: The location to copy the file to
        """
        # Download granule from external S3 bucket with provided keys
        external_s3_client = get_s3_client_with_keys(self.key_id_name, self.secret_key_name)
        bucket_and_key = source_s3_uri.replace('s3://', '').split('/', 1)
        download_path = os.getenv('efs_path')
        filename = f'{download_path}/{bucket_and_key[-1].rsplit("/" , 1)[-1]}'
        external_s3_client.download_file(Bucket=bucket_and_key[0], Filename=filename, Key=bucket_and_key[-1])

        # Upload from ec2 to internal S3 then delete the copied file
        internal_s3_client = get_s3_client()
        if not destination_bucket:
            destination_bucket = f'{os.getenv("stackName")}-private'
            # rdg_logger.info(f'key: {bucket_and_key[-1]}')
        internal_s3_client.upload_file(Bucket=destination_bucket, Filename=filename, Key=bucket_and_key[-1])
        try:
            os.remove(filename)
        except FileNotFoundError:
            rdg_logger.warning(f'Failed to delete {filename}. File does not exist.')

    def move_granule_wrapper(self, granule_list_dicts):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for granule_dict in granule_list_dicts:
                futures.append(
                    executor.submit(self.move_granule, granule_dict.get('name'))
                )

            for future in concurrent.futures.as_completed(futures):
                future.result()


if __name__ == '__main__':
    pass
