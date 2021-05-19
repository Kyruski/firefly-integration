from __future__ import annotations

from datetime import datetime
from hashlib import md5
from typing import List

import awswrangler as wr
import firefly as ff
import pandas as pd
from botocore.exceptions import ClientError

import firefly_integration.domain as domain
from firefly_integration.domain.service.dal import Dal

MAX_FILE_SIZE = 1000000000  # ~1GB
PARTITION_LOCK = 'partition-lock-{}'


class AwsDal(Dal):
    _batch_process: ff.BatchProcess = None
    _remove_duplicates: domain.RemoveDuplicates = None
    _mutex: ff.Mutex = None
    _db_created: dict = {}
    _context: str = None
    _bucket: str = None

    def store(self, df: pd.DataFrame, table: domain.Table):
        self._ensure_db_created(table)

        df = df[list(map(lambda c: c.name, table.columns))]

        if 'created_on' in table.type_dict:
            df['created_on'].fillna(datetime.now(), inplace=True)
        if 'updated_on' in table.type_dict:
            df['updated_on'] = datetime.now()

        params = {
            'df': df,
            'path': table.full_path(),
            'dataset': True,
            'database': table.database.name,
            'table': table.name,
            'partition_cols': table.partition_columns,
            'compression': 'snappy',
            'dtype': table.type_dict,
            'schema_evolution': True,
        }

        if table.time_partitioning is not None:
            params['projection_enabled'] = True
            params['regular_partitions'] = True
            if table.time_partitioning == 'year':
                params['projection_types'] = {'year': 'integer'}
                params['projection_ranges'] = {'year': f'2000,2100'}
            elif table.time_partitioning == 'month':
                params['projection_types'] = {'year': 'integer', 'month': 'integer'}
                params['projection_ranges'] = {'year': f'2000,2100', 'month': '1,12'}
            elif table.time_partitioning == 'day':
                params['projection_types'] = {'year': 'integer', 'month': 'integer', 'day': 'integer'}
                params['projection_ranges'] = {'year': f'2000,2100', 'month': '1,12', 'day': '1,31'}

        wr.s3.to_parquet(**params)

    def load(self, table: domain.Table, criteria: ff.BinaryOp = None) -> pd.DataFrame:
        pass

    def delete(self, criteria: ff.BinaryOp, table: domain.Table):
        pass

    def get_partitions(self, table: domain.Table, criteria: ff.BinaryOp = None) -> List[str]:
        args = {'database': table.database.name, 'table': table.name}
        if criteria is not None:
            args['expression'] = str(criteria)

        try:
            partitions = wr.catalog.get_parquet_partitions(**args)
        except ClientError:
            return []

        return list(map(lambda p: p.replace('s3://', ''), partitions.keys()))

    def wait_for_tmp_files(self, files: list):
        wr.s3.wait_objects_exist(
            list(map(lambda f: f's3://{self._bucket}/{f}', files)),
            delay=1,
            max_attempts=60,
            use_threads=True
        )

    def read_tmp_files(self, files: list) -> pd.DataFrame:
        return wr.s3.read_parquet(list(map(lambda f: f's3://{self._bucket}/{f}', files)), use_threads=True)

    def write_tmp_file(self, file: str, data: pd.DataFrame):
        wr.s3.to_parquet(data, path=f's3://{self._bucket}/{file}')

    def compact(self, table: domain.Table, path: str):
        path = path.rstrip('/')
        if not path.startswith('s3://'):
            path = f's3://{path}'

        ignore = []
        to_delete = []
        key = None
        n = 0
        for k, size in wr.s3.size_objects(path=f'{path}/', use_threads=True).items():
            if k.endswith('.dat.snappy.parquet'):
                if size >= MAX_FILE_SIZE:
                    ignore.append(f'{k.split("/")[-1]}')
                else:
                    key = k
                n += 1
            else:
                to_delete.append(k)

        if len(to_delete) == 0:
            return  # Nothing new to compact

        with self._mutex(PARTITION_LOCK.format(md5(path.encode('utf-8')))):
            if key is None:
                key = f'{path}/{n + 1}.dat.snappy.parquet'

            df = wr.s3.read_parquet(path=path, path_ignore_suffix=ignore, use_threads=True)
            self._remove_duplicates(df, table)
            df.reset_index(inplace=True)
            wr.s3.to_parquet(df=df, path=key, compression='snappy', dtype=table.type_dict, use_threads=True)
            wr.s3.delete_objects(to_delete, use_threads=True)

    def _ensure_db_created(self, table: domain.Table):
        if table.database.name not in self._db_created:
            wr.catalog.create_database(name=table.database.name, exist_ok=True,
                                       description=table.database.description or '')
            self._db_created[table.database.name] = True
