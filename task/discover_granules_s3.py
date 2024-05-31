import concurrent.futures
import os
import re

import boto3

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import gdg_logger

ONE_MEBIBIT = 1048576


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


def get_s3_resp_iterator(host, prefix, s3_client, pagination_config=None, start_after=''):
    """
    Returns an s3 paginator.
    :param host: The bucket.
    :param prefix: The path for the s3 granules.
    :param s3_client: Initialized boto3 S3 client
    :param pagination_config: Configuration for s3 pagination
    :param start_after: S3 key to start pagination
    """
    if pagination_config is None:
        pagination_config = {'page_size': 1000}

    s3_paginator = s3_client.get_paginator('list_objects_v2')
    return s3_paginator.paginate(
        Bucket=host,
        Prefix=prefix,
        PaginationConfig=pagination_config,
        StartAfter=start_after
    )


class DiscoverGranulesS3(DiscoverGranulesBase):
    """
    Class to discover granules from S3 provider
    """
    def __init__(self, event, context):
        super().__init__(event, context=context)
        self.key_id_name = self.meta.get('aws_key_id_name')
        self.secret_key_name = self.meta.get('aws_secret_key_name')
        self.prefix = str(self.collection['meta']['provider_path']).lstrip('/')
        self.bookmark = self.discover_tf.get('bookmark', '')
        self.early_return_threshold = int(os.getenv('early_return_threshold', 0)) * 1000

    def discover_granules(self):
        ret = {}
        try:
            gdg_logger.info(f'Discovering in {self.provider_url}')
            s3_client = get_s3_client() if None in [self.key_id_name, self.secret_key_name] \
                else get_s3_client_with_keys(self.key_id_name, self.secret_key_name)
            start_after = self.discover_tf.get('bookmark', '')
            self.bookmark = self.discover(get_s3_resp_iterator(
                self.host, self.prefix, s3_client, start_after=start_after)
            )
            self.dbm.flush_dict()
            if not self.bookmark:
                gdg_logger.info('Reading batch')
                batch = self.dbm.read_batch()
                # ret.update({'batch': batch})
                ret.update({
                    'batch': batch,
                    'discovered_files_count': self.dbm.discovered_files_count + self.discovered_files_count,
                    'queued_files_count': self.dbm.queued_files_count
                })
            else:
                ret.update({
                    'bookmark': self.bookmark,
                    'discovered_files_count': self.dbm.discovered_files_count + self.discovered_files_count,
                    'queued_files_count': 0
                })
        except ValueError as e:
            gdg_logger.error(e)
            raise
        finally:
            self.dbm.close_db()

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
                last_s3_key = s3_object["Key"]
                key = f'{self.provider.get("protocol")}://{self.provider.get("host")}/{s3_object["Key"]}'
                sections = str(key).rsplit('/', 1)
                key_dir = sections[0]
                url_segment = sections[1]
                if check_reg_ex(self.granule_id_extraction, url_segment) and check_reg_ex(self.dir_reg_ex, key_dir):
                    etag = s3_object['ETag'].strip('"')
                    last_modified = s3_object['LastModified']
                    size = int(s3_object['Size'])
                    # print(f'Found: {key}')
                    reg_res = re.search(self.granule_id_extraction, url_segment)
                    if reg_res:
                        granule_id = reg_res.group(1)
                        self.dbm.add_record(
                            name=key, granule_id=granule_id,
                            collection_id=self.collection_id, etag=etag,
                            last_modified=last_modified, size=size
                        )

                    else:
                        # gdg_logger.warning(
                        #     f'The collection\'s granuleIdExtraction {self.granule_id_extraction}'
                        #     f' did not match the filename {url_segment}.'
                        # )
                        pass

                # Check Time
                # time_remaining = self.lambda_context.get_remaining_time_in_millis()
                # if time_remaining < self.early_return_threshold:
                #     gdg_logger.info(f'Doing early return. Last key: {last_s3_key}')
                #     return last_s3_key

        return None

    def move_granule(
            self, s3_client_source, s3_client_destination, granule_dict,
            destination_bucket=f'{os.getenv("stackName")}-private'
    ):
        """
        Copies a file from a source S3 client to a destination S3 client
        :param s3_client_source: Source S3 client
        :param s3_client_destination: Destination S3 client
        :param granule_dict: granule dictionary contained needed name and size fields
        :param destination_bucket: The location to copy the file to
        """
        source_s3_uri = granule_dict.get('name')
        size = int(granule_dict.get('size'))

        bucket_key_regex = re.compile('(?:s3://)([^/]*)/(.*)')
        regex_res = bucket_key_regex.search(source_s3_uri)
        source_bucket = regex_res.group(1)
        key = regex_res.group(2)

        s3_stream = s3_client_source.get_object(
            Bucket=source_bucket,
            Key=key
        ).get('Body')

        # The default part size is 8MB. Further testing needs to be done to determine optimal size.
        if size >= (ONE_MEBIBIT * 8):
            self.multipart_upload(s3_stream, s3_client_destination, destination_bucket, key)
        else:
            s3_client_destination.put_object(Bucket=destination_bucket, Body=s3_stream.read(), Key=key)

    @staticmethod
    def multipart_upload(stream_iter, destination_client, bucket, key):
        mp_upload_args = {
            'Key': key,
            'Bucket': bucket
        }

        rsp = destination_client.create_multipart_upload(**mp_upload_args)
        mp_upload_args.update({'UploadId': rsp.get('UploadId')})

        parts = []
        part_number = 1
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for chunk in stream_iter.iter_chunks(ONE_MEBIBIT * 30):
                upload_part_args = {**mp_upload_args, **{'Body': chunk, 'PartNumber': part_number}}
                part_dict = {
                    'PartNumber': part_number
                }
                futures.append(
                    executor.submit(destination_client.upload_part, **upload_part_args)
                )
                parts.append(part_dict)
                part_number += 1

            futures_res = concurrent.futures.wait(futures)
            if futures_res.not_done:
                raise ValueError('Not all futures completed.')
            else:
                for future_index in range(len(futures)):
                    etag = futures[future_index].result().get('ETag')
                    parts[future_index].update({'ETag': etag})

        mp_upload_args.update({'MultipartUpload': {'Parts': parts}})
        rsp = destination_client.complete_multipart_upload(**mp_upload_args)

        # TODO: Make this option configurable
        # Copy over self to convert multipart upload ETag to normal ETag. The performance impact of this is negligible
        # cp_rsp = destination_client.copy_object(
        #     Bucket='sharedsbx-private',
        #     Key=key,
        #     CopySource={
        #         'Bucket': 'sharedsbx-private',
        #         'Key': key
        #     }
        # )
        # print(cp_rsp)
        return rsp

    def move_granule_wrapper(self, granule_list_dicts):
        gdg_logger.info(f'Moving granules to internal bucket')
        external_s3_client = get_s3_client_with_keys(self.key_id_name, self.secret_key_name)
        internal_s3_client = get_s3_client()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for granule_dict in granule_list_dicts:
                futures.append(
                    executor.submit(self.move_granule, external_s3_client, internal_s3_client, granule_dict)
                )

            for future in concurrent.futures.as_completed(futures):
                future.result()


if __name__ == '__main__':
    pass
