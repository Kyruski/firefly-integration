from __future__ import annotations

from datetime import datetime
from typing import List

import awswrangler as wr
import firefly as ff
import pandas as pd

import firefly_integration.domain as domain
from firefly_integration.domain.service.dal import Dal


class AwsDal(Dal):
    _batch_process: ff.BatchProcess = None
    _db_created: dict = {}
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
        partitions = wr.catalog.get_parquet_partitions(**args)

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

    def _ensure_db_created(self, table: domain.Table):
        if table.database.name not in self._db_created:
            wr.catalog.create_database(name=table.database.name, exist_ok=True,
                                       description=table.database.description or '')
            self._db_created[table.database.name] = True
