from __future__ import annotations

import uuid
from datetime import datetime, date
from typing import List, Dict, Callable, Union

import inflection
import pandas as pd

import firefly_integration.domain as domain


class Table:
    name: str = None
    path: str = None
    description: str = None
    columns: List[domain.Column] = []
    partitions: List[domain.Column] = []
    duplicate_fields: List[str] = []
    duplicate_sort: List[str] = []
    database: domain.Database = None
    date_grouping: dict = None  # ???
    time_partitioning: str = None
    time_partitioning_column: str = None
    file_name: Callable = None
    _partition_generators: Dict[str, Callable] = None

    def __init__(self, name: str, columns: List[domain.Column], partitions: List[domain.Column] = None, path: str = '',
                 description: str = None, duplicate_fields: List[str] = None, duplicate_sort: List[str] = None,
                 partition_generators: Dict[str, Callable] = None, date_grouping: dict = None,
                 file_name: Callable = None, time_partitioning: str = None, time_partitioning_column: str = None):
        self.name = inflection.tableize(name)
        self.path = path
        self.columns = columns
        self.description = description
        self.partitions = partitions or []
        self.duplicate_fields = duplicate_fields or ['id']
        self.duplicate_sort = duplicate_sort or []
        self._partition_generators = partition_generators
        self.date_grouping = date_grouping
        self.file_name = file_name
        self.time_partitioning = time_partitioning
        self.time_partitioning_column = time_partitioning_column

        for column in self.columns:
            column.table = self

    def get_column(self, name: str):
        for column in self.columns:
            if column.name == name:
                return column
        raise domain.ColumnNotFound(name)

    def generate_partition_path(self, data: pd.DataFrame):
        parts = []
        for partition in self.partitions:
            if self._partition_generators is not None and partition.name in self._partition_generators:
                parts.append(f'{partition.name}={self._partition_generators[partition.name](data)}')
            elif partition.name in data:
                parts.append(f'{partition.name}={data[partition.name]}')
            else:
                raise domain.InvalidPartitionData()

        return '/'.join(parts)

    @property
    def type_dict(self):
        ret = {}
        for column in self.columns:
            ret[column.name] = self._pandas_type(column.data_type)
        return ret

    @property
    def partition_columns(self):
        return list(map(lambda c: c.name, self.partitions))

    def full_path(self, df: pd.DataFrame = None):
        ret = f'{self.database.path}/{self.path or ""}'.rstrip('/')
        ret = f'{ret}/{self.name}'
        if df is not None:
            ret = f'{ret}/{self.generate_partition_path(df)}'
            if self.file_name:
                ret = f'{ret}/{self.file_name(df)}'
            else:
                ret = f'{ret}/{str(uuid.uuid4())}'
        if df is not None:
            ret = f'{ret}.snappy.parquet'

        return ret

    @property
    def time_partition_format(self):
        ret = None
        if self.time_partitioning is not None:
            ret = '%Y'
            if self.time_partitioning == 'month':
                ret += '-%m'
            if self.time_partitioning == 'day':
                ret += '-%m-%d'

        return ret

    def _pandas_type(self, t: type):
        if t is str:
            return 'string'
        if t is int:
            return 'bigint'
        if t is float:
            return 'double'
        if t is bool:
            return 'boolean'
        if t is datetime:
            return 'timestamp'
        if t is date:
            return 'date'
