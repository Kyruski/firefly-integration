from __future__ import annotations

from abc import ABC

import firefly as ff

import firefly_integration.domain as domain
import awswrangler as wr

MAX_FILE_SIZE = 1000000000  # ~1GB


class Compactor(ff.ApplicationService, ABC):
    _context: str = None

    def __init__(self, table: domain.Table):
        self._table = table

    def __call__(self, *args, **kwargs):
        partitions = wr.catalog.get_parquet_partitions(database=self._table.database.name, table=self._table.name)

        for partition, values in partitions:
            self.invoke(f'{self._context}.CompactPath', {
                'path': partition,
                'type_dict': self._table.type_dict,
                'sort_fields': self._table.duplicate_sort,
                'unique_fields': self._table.duplicate_fields,
            })

        print(partitions)


class CompactPath(ff.DomainService):
    _remove_duplicates: domain.RemoveDuplicates = None

    def __call__(self, path: str, type_dict: dict, sort_fields: list, unique_fields: list):
        ignore = []
        to_delete = []
        key = None
        n = 0
        for k, size in wr.s3.size_objects(path=path, use_threads=True).items():
            if k.endswith('.dat.snappy.parquet'):
                if size >= MAX_FILE_SIZE:
                    ignore.append(f'{k.split("/")[-1]}')
                else:
                    key = k
                n += 1
            else:
                to_delete.append(k)

        if key is None:
            key = f'{path}/__{n + 1}.dat.snappy.parquet'

        print(f'Key: {key}')

        df = wr.s3.read_parquet(path=path, path_ignore_suffix=ignore, use_threads=True)
        print(df)
        self._remove_duplicates(df, sort_fields, unique_fields)
        print(df)
        wr.s3.to_parquet(df=df, path=key, compression='snappy', dtype=type_dict, use_threads=True)
        print(f'Delete: {to_delete}')
        # wr.s3.delete_objects(to_delete, use_threads=True)
