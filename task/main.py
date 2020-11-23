class DiscoverGranules:
    """
    This class contains functions that fetch
    The metadata of the granules via a protocol X (HTTP/SFTP/S3)
    Compare the md5 of these granules with the ones in an S3
    It will return the files if they don't exist in S3 or the md5 doesn't match
    """

    def __init__(self):
        """
        Default values goes here
        """
        # Implement me
        pass

    def get_files_link_http(self, url_path, reg_ex=None):
        """
        Fetch the link of the granules in the host url_path
        :param url_path: The base URL where the files are served
        :type url_path: string
        :param reg_ex: Regular expression to match the files to be added
        :type reg_ex: string
        :return: links of files matching reg_ex (if reg_ex is defined)
        :rtype: list of urls
        """
        # Implement me
        pass

