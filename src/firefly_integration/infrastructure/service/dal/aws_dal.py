from __future__ import annotations

from datetime import datetime
from hashlib import md5
from typing import List, Tuple

import awswrangler as wr
import firefly as ff
import pandas as pd
from botocore.exceptions import ClientError

import firefly_integration.domain as domain
from firefly_integration.domain.service.dal import Dal

MAX_FILE_SIZE = 262144000  # 250MB
MAX_RUN_TIME = 600  # 10 Minutes
PARTITION_LOCK = 'partition-lock-{}'


class AwsDal(Dal, ff.LoggerAware):
    _batch_process: ff.BatchProcess = None
    _remove_duplicates: domain.RemoveDuplicates = None
    _sanitize_input_data: domain.SanitizeInputData = None
    _s3_client = None
    _mutex: ff.Mutex = None
    _db_created: dict = {}
    _context: str = None
    _bucket: str = None
    _max_compact_records: str = None

    def __init__(self):
        super().__init__()
        if self._max_compact_records is None:
            self._max_compact_records = '1000'

    def store(self, df: pd.DataFrame, table: domain.Table):
        self._ensure_db_created(table)

        df = df[list(map(lambda c: c.name, table.columns))]

        if 'created_on' in table.type_dict:
            df['created_on'].fillna(datetime.utcnow(), inplace=True)
        if 'updated_on' in table.type_dict:
            df['updated_on'] = datetime.utcnow()

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
            params['partition_cols'] = ['dt']
            # params['projection_enabled'] = True
            params['regular_partitions'] = True
            fmt = '%Y'
            if table.time_partitioning == 'month':
                fmt += '-%m'
            if table.time_partitioning == 'day':
                fmt += '-%m-%d'

            df['dt'] = pd.to_datetime(df[table.time_partitioning_column]).dt.strftime(fmt)

            # params['projection_types'] = {'dt': 'enum'}
            # params['projection_values'] = {'dt': self._date_partition_range(df, table.time_partitioning)}

        if not df.empty:
            wr.s3.to_parquet(**params)

    def _date_partition_range(self, df: pd.DataFrame, timeframe: str):
        start = df['dt'].min()
        end = df['dt'].max()
        fmt = '%Y'
        if timeframe == 'month':
            fmt += '-%m'
        if timeframe == 'day':
            fmt += '-%m-%d'

        return ','.join(pd.date_range(start, end, freq='MS').strftime(fmt).to_list())

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

    def deduplicate_partition(self, table: domain.Table, path: str):
        if table.duplicate_sort is None or table.duplicate_fields is None:
            return

        path = self._prepare_path(path)
        dt = None
        for x in path.split('/'):
            if x.startswith('dt='):
                dt = x.split('=')[1]
                break
        fields: list = table.duplicate_fields.copy() \
            if isinstance(table.duplicate_fields, list) else [table.duplicate_fields]
        if isinstance(table.duplicate_sort, list):
            fields.extend(table.duplicate_sort)
        else:
            fields.append(table.duplicate_sort)
        fields = list(map(lambda f: f'a."{f}"', fields))

        inner_clauses = [f' {f} is not null ' for f in table.duplicate_fields]
        outer_clauses = [f' a."{f}" = b."{f}" ' for f in table.duplicate_fields]
        dt_clause = f"dt = '{dt}' and " if dt is not None else ''

        sql = f"""
select {','.join(fields)}, a."$path"
from {table.name} a
join (
  select {','.join(table.duplicate_fields)}, count(*)
  from {table.name}
  where {dt_clause}
    "$path" like '%.dat.snappy.parquet'
    and {'and'.join(inner_clauses)}
  group by {','.join(table.duplicate_fields)}
  having count(*) > 1
) b
on {'and'.join(outer_clauses)}
where {dt_clause}
  a."$path" like '%.dat.snappy.parquet'
order by {','.join(table.duplicate_fields)}
        """

        df = wr.athena.read_sql_query(sql=sql, database=table.database.name, ctas_approach=False)
        if df.empty:
            return

        df.sort_values(by=table.duplicate_sort, inplace=True)
        df['duplicate'] = df.duplicated(subset=table.duplicate_fields, keep='last')
        df = df[df['duplicate']]
        df['u'] = df['updated_on'].astype('datetime64[s]')
        join_fields = table.duplicate_fields.copy() \
            if isinstance(table.duplicate_fields, list) else [table.duplicate_fields]
        join_fields.append('u')

        try:
            with self._mutex(PARTITION_LOCK.format(md5(path.encode('utf-8')).hexdigest())):
                start = datetime.now()
                for _, batch in df.groupby('$path'):
                    if (datetime.now() - start).total_seconds() >= MAX_RUN_TIME:
                        self.info("We've been running for 10 minutes. Stopping now.")
                        break

                    p = batch.iloc[0]['$path']
                    print(f'Filtering {p}')
                    f = wr.s3.read_parquet(path=p)
                    if 'updated_on' not in f:
                        self.info('No updated_on field in record set')
                        continue
                    f['u'] = f['updated_on'].astype('datetime64[s]')
                    f = pd.merge(f, batch, indicator=True, how='outer', left_on=join_fields, right_on=join_fields)\
                        .query('_merge == "left_only"')\
                        .drop('_merge', axis=1)
                    wr.s3.to_parquet(
                        df=f, path=f'{p}.tmp', compression='snappy', dtype=table.type_dict, use_threads=True
                    )
                    dir_ = '/'.join(p.split('/')[0:-1]) + '/'
                    file_ = p.split('/')[-1]
                    wr.s3.copy_objects(paths=[f'{p}.tmp'], source_path=dir_, target_path=dir_, replace_filenames={
                        f'{file_}.tmp': file_,
                    })
                    wr.s3.delete_objects(path=[f'{p}.tmp'])
        except TimeoutError:
            pass

    def compact(self, table: domain.Table, path: str):
        start = datetime.now()
        while True:
            if self._do_compact(table, path) is True:
                break
            if (datetime.now() - start).total_seconds() >= MAX_RUN_TIME:
                self.info("We've been running for 10 minutes. Stopping now.")
                break

    def _do_compact(self, table: domain.Table, path: str) -> bool:
        path = self._prepare_path(path)
        parts = path.split('/')
        bucket = parts[2]
        p = '/'.join(parts[3:])
        key, key_exists, num_master_records, master_record_size = self._find_master_record(bucket, p)
        to_compact = self._find_files_to_compact(bucket, p, num_master_records, master_record_size)

        if len(to_compact) == 0:
            return True  # Nothing new to compact

        to_read = to_compact.copy()
        if key_exists:
            to_read.append(f's3://{bucket}/{key}')

        try:
            print(PARTITION_LOCK.format(md5(path.encode('utf-8')).hexdigest()))
            with self._mutex(PARTITION_LOCK.format(md5(path.encode('utf-8')).hexdigest()), timeout=0):
                try:
                    df = self._sanitize_input_data(wr.s3.read_parquet(path=to_read, use_threads=True), table)
                except ClientError:
                    return True  # Another process must be compacting, so stop running

                self._remove_duplicates(df, table)
                try:
                    df.reset_index(inplace=True)
                except ValueError:
                    pass
                wr.s3.to_parquet(
                    df=df, path=f's3://{bucket}/{key}.tmp', compression='snappy', dtype=table.type_dict,
                    use_threads=True
                )
                self._s3_client.copy_object(Bucket=bucket, CopySource=f'{bucket}/{key}.tmp', Key=key)
                self._s3_client.delete_object(Bucket=bucket, Key=f'{key}.tmp')
                wr.s3.delete_objects(to_compact, use_threads=True)

                self.info(f'Compacted {len(to_compact)} records')
        except TimeoutError:
            return True

        return False

    def _find_master_record(self, bucket: str, key: str) -> Tuple[str, bool, int, int]:
        x = 1
        while True:
            try:
                response = self._s3_client.head_object(Bucket=bucket, Key=f'{key}/{x}.dat.snappy.parquet')
                if int(response['ContentLength']) < MAX_FILE_SIZE:
                    return f'{key}/{x}.dat.snappy.parquet', True, x, int(response['ContentLength'])
            except ClientError:
                return f'{key}/{x}.dat.snappy.parquet', False, x, 0
            x += 1

    def _find_files_to_compact(self, bucket: str, key: str, num_master_records: int, master_record_size: int):
        response = self._s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f'{key}/',
            MaxKeys=int(self._max_compact_records) + num_master_records
        )
        ret = []
        total = master_record_size
        try:
            for f in response['Contents']:
                if '.dat.snappy.parquet' in f['Key']:
                    continue
                total += f['Size']
                ret.append(f's3://{bucket}/{f["Key"]}')
                if total > MAX_FILE_SIZE:
                    break
        except KeyError:
            pass

        return ret

    def _ensure_db_created(self, table: domain.Table):
        if table.database.name not in self._db_created:
            wr.catalog.create_database(name=table.database.name, exist_ok=True,
                                       description=table.database.description or '')
            self._db_created[table.database.name] = True

    def _prepare_path(self, path: str):
        path = path.rstrip('/')
        if not path.startswith('s3://'):
            path = f's3://{path}'

        return path
