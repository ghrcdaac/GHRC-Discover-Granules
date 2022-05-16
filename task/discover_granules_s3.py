from task.discover_granules_base import DiscoverGranulesBase
import boto3
import re


class DiscoverGranulesS3(DiscoverGranulesBase):
    """
    Class to discover granules from S3 provider
    """

    def __init__(self, event, logger):
        super().__init__(event, logger)
        key_id_name = self.meta.get('aws_key_id_name')
        secret_key_name = self.meta.get('aws_secret_key_name')
        self.s3_client = self.get_s3_client() if None in [key_id_name, secret_key_name] \
            else self.get_s3_client_with_keys(key_id_name, secret_key_name)

    @staticmethod
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

    def get_s3_client_with_keys(self, key_id_name, secret_key_name):
        """
        Gets a boto3 s3 client using an aws key id and secret key if provided
        :param key_id_name: ID of the aws key
        :param secret_key_name: Name of the aws key
        """
        ssm_client = boto3.client('ssm')
        id_key = lambda id_name: ssm_client.get_parameter(Name=id_name).get('value')
        return self.get_s3_client(aws_key_id=id_key(key_id_name), aws_secret_key=id_key(secret_key_name))

    def discover_granules(self):
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
        host = self.host
        prefix = self.collection['meta']['provider_path']
        file_reg_ex = self.collection.get('granuleIdExtraction')
        dir_reg_ex = self.discover_tf.get('dir_reg_ex')
        self.logger.info(f'Discovering in s3://{host}/{prefix}.')
        response_iterator = self.get_s3_resp_iterator(host, prefix)
        ret_dict = {}
        for page in response_iterator:
            for s3_object in page.get('Contents', {}):
                key = f'{self.provider.get("protocol")}://{self.provider.get("host")}/{s3_object["Key"]}'
                sections = str(key).rsplit('/', 1)
                key_dir = sections[0]
                file_name = sections[1]
                if (file_reg_ex is None or re.search(file_reg_ex, file_name)) and \
                        (dir_reg_ex is None or re.search(dir_reg_ex, key_dir)):
                    etag = s3_object['ETag'].strip('"')
                    last_modified = s3_object['LastModified'].timestamp()
                    size = s3_object['Size']

                    # rdg_logger.info(f'Found granule: {key}')
                    # rdg_logger.info(f'ETag: {etag}')
                    # rdg_logger.info(f'Last-Modified: {last_modified}')

                    self.populate_dict(ret_dict, key, etag, last_modified, size)

        return ret_dict

    def get_s3_resp_iterator(self, host, prefix):
        """
        Returns an s3 paginator.
        :param host: The bucket.
        :param prefix: The path for the s3 granules.
        """
        s3_paginator = self.s3_client.get_paginator('list_objects')
        return s3_paginator.paginate(
            Bucket=host,
            Prefix=prefix,
            PaginationConfig={
                'PageSize': 1000
            }
        )
