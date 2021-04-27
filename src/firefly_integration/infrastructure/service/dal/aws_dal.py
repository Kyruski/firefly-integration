from __future__ import annotations

from datetime import datetime

import awswrangler as wr
import firefly as ff
import pandas as pd

import firefly_integration.domain as domain
from firefly_integration.domain.service.dal import Dal


class AwsDal(Dal):
    _batch_process: ff.BatchProcess = None
    _db_created: dict = {}

    def store(self, df: pd.DataFrame, table: domain.Table):
        self._ensure_db_created(table)

        try:
            table.get_column('created_on')
            df['created_on'] = datetime.now()
        except domain.ColumnNotFound:
            pass

        params = {
            'df': df,
            'path': table.full_path(),
            'dataset': True,
            'database': table.database.name,
            'table': table.name,
            'partition_cols': table.partition_columns,
            'compression': 'snappy',
            'dtype': table.type_dict
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
                params['projection_digits'] = {'month': 2}
            elif table.time_partitioning == 'day':
                params['projection_types'] = {'year': 'integer', 'month': 'integer', 'day': 'integer'}
                params['projection_ranges'] = {'year': f'2000,2100', 'month': '1,12', 'day': '1,31'}
                params['projection_digits'] = {'month': 2, 'day': 2}

        wr.s3.to_parquet(**params)

    def load(self, table: domain.Table, criteria: ff.BinaryOp = None) -> pd.DataFrame:
        pass

    def delete(self, criteria: ff.BinaryOp, table: domain.Table):
        pass

    def _ensure_db_created(self, table: domain.Table):
        if table.database.name not in self._db_created:
            wr.catalog.create_database(name=table.database.name, exist_ok=True,
                                       description=table.database.description or '')
            self._db_created[table.database.name] = True
