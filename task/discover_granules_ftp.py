import io
from contextlib import redirect_stdout
from ftplib import FTP

from task.discover_granules_base import DiscoverGranulesBase, check_reg_ex
from task.logger import rdg_logger


def setup_ftp_client(**kwargs):
    ftp = FTP(host=kwargs.get('host'))
    ftp.connect(port=kwargs.get('port', 21))
    ftp.login(user=kwargs.get('user', ''))
    return ftp


class DiscoverGranulesFTP(DiscoverGranulesBase):
    """
    Class to discover granules from an FTP provider
    """
    def __init__(self, event):
        super().__init__(event)
        self.path = self.config.get('provider_path')
        self.depth = self.discover_tf.get('depth')
        self.provider_url = f'{self.provider["protocol"]}://{self.host.rstrip("/")}/' \
                            f'{self.config["provider_path"].lstrip("/")}'

    def discover_granules(self):
        try:
            ftp_client = setup_ftp_client(**self.provider)
            self.discover(ftp_client)
            self.dbm.flush_dict()
            batch = self.dbm.read_batch(self.collection_id, self.provider_url, self.discover_tf.get('batch_limit'))
        finally:
            self.dbm.close_db()

        ret = {
            'discovered_files_count': self.dbm.discovered_files_count + self.discovered_files_count,
            'queued_files_count': self.dbm.queued_files_count,
            'batch': batch
        }

        return ret

    def discover(self, ftp_client):
        rdg_logger.info(f'Discovering in {self.provider_url}')
        ftp_client.cwd(self.path)

        directory_list = []
        with io.StringIO() as buffer, redirect_stdout(buffer):
            ftp_client.retrlines('LIST')
            output = buffer.getvalue()

        output_rows = output.splitlines()
        for row in output_rows:
            column_list = row.split()
            filename = column_list[-1]
            if row.startswith('d') and check_reg_ex(self.dir_reg_ex, self.path):
                rdg_logger.info(f'{filename} was a directory')
                directory_list.append(filename)
            elif check_reg_ex(self.granule_id_extraction, str(filename)):
                if len(column_list) == 9:
                    size = column_list[4]
                    last_mod = ' '.join(column_list[5:8])
                    
                    self.dbm.add_record(
                        name=filename, granule_id=filename, collection_id='collection_id',
                        etag='N/A', last_modified=last_mod, size=size
                    )
                else:
                    raise Exception(f'FTP row format is not the expected length: {column_list}')
            else:
                rdg_logger.info(f'The granuleIdExtraction {self.granule_id_extraction} did not match the file name.')

        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.path = directory

        ftp_client.cwd('../')


if __name__ == "__main__":
    pass
