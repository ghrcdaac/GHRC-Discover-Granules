import io
import re
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
    def __init__(self, event, context):
        super().__init__(event, context=context)
        self.provider_path = self.config.get('provider_path')
        self.depth = self.discover_tf.get('depth')

    def discover_granules(self):
        try:
            ftp_client = setup_ftp_client(**self.provider)
            self.discover(ftp_client)
            ftp_client.close()
            self.dbm.flush_dict()
            batch = self.dbm.read_batch()
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
        ftp_client.cwd(self.provider_path)
        directory_list = []
        with io.StringIO() as buffer, redirect_stdout(buffer):
            ftp_client.retrlines('LIST')
            output = buffer.getvalue()

        self.process_ftp_list_output(output, directory_list)

        if self.depth > 0:
            self.depth -= 1
            for directory in directory_list:
                self.provider_path = directory
                print(f'new_path: {self.provider_path}')

        ftp_client.cwd('../')

    def process_ftp_list_output(self, output, directory_list):
        output_rows = output.splitlines()
        for row in output_rows:
            column_list = row.split()
            filename = column_list[-1]
            if row.startswith('d') and check_reg_ex(self.dir_reg_ex, self.provider_path):
                # rdg_logger.info(f'{filename} was a directory')
                directory_list.append(filename)
            else:
                granule_id_match = re.search(self.granule_id_extraction, str(filename))
                if granule_id_match:
                    if len(column_list) == 9:
                        size = column_list[4]
                        last_mod = ' '.join(column_list[5:8])
                        full_url = f'{self.provider_url}{filename}'
                        self.dbm.add_record(
                            name=full_url, granule_id=granule_id_match.group(), collection_id=self.collection_id,
                            etag='N/A', last_modified=last_mod, size=size
                        )
                    else:
                        raise ValueError(f'FTP row format is not the expected length: {column_list}')
                else:
                    # rdg_logger.info(f'The granuleIdExtraction {self.granule_id_extraction} did not match the file name.')
                    pass


if __name__ == "__main__":
    pass
