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


class DBManagerWriter(DBManagerBase, ABC):
    def __init__(
            self, db_type, duplicate_handling, transaction_size, cumulus_filter_dbm=None,
            auto_batching=True, **kwargs
    ):
        super().__init__(db_type, duplicate_handling, transaction_size, **kwargs)
        self.auto_batching = auto_batching
        self.cumulus_filter_dbm = cumulus_filter_dbm

    def add_record(self, name, granule_id, collection_id, etag, last_modified, size):
        super().add_record(name, granule_id, collection_id, etag, last_modified, size)

        if self.auto_batching and len(self.dict_list) >= self.transaction_size:
            self.write_batch()

        return self.transaction_size - len(self.dict_list)

    def flush_dict(self):
        self.write_batch()

    def write_batch(self):
        if self.cumulus_filter_dbm and self.duplicate_handling == 'skip':
            discovered_granule_ids = tuple(x.get('granule_id') for x in self.dict_list)
            new_granule_ids = self.cumulus_filter_dbm.filter_against_cumulus(discovered_granule_ids)

            index = 0
            while index < len(self.dict_list):
                record = self.dict_list[index]
                if record.get('granule_id') not in new_granule_ids:
                    self.dict_list.pop(index)
                else:
                    index += 1

        print(f'Writing batch to database...')
        print(self.dict_list)
        if len(self.dict_list) > 0:
            self.discovered_files_count += getattr(self.model, f'db_{self.duplicate_handling}')(self.dict_list)
            self.dict_list.clear()

    def read_batch(self, collection_id, provider_path, batch_size):
        batch = self.model.read_batch(collection_id, provider_path, batch_size)
        self.queued_files_count += len(batch)
        return batch


