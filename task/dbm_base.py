from abc import ABC, abstractmethod


class DBManagerBase(ABC):
    def __init__(self, db_type, duplicate_handling='skip', transaction_size=100000, **kwargs):
        self.db_type = db_type
        self.dict_list = []
        self.discovered_files_count = 0
        self.queued_files_count = 0
        self.duplicate_handling = duplicate_handling
        self.transaction_size = transaction_size

    @abstractmethod
    def close_db(self):
        raise NotImplementedError

    def add_record(self, name, granule_id, collection_id, etag, last_modified, size):
        self.dict_list.append({
            'name': name,
            'etag': etag,
            'granule_id': granule_id,
            'collection_id': collection_id,
            'last_modified': str(last_modified),
            'size': size
        })

    @abstractmethod
    def flush_dict(self):
        raise NotImplementedError

    @abstractmethod
    def read_batch(self, collection_id, provider_path, batch_size):
        raise NotImplementedError