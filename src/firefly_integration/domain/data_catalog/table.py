from __future__ import annotations

from datetime import datetime, date
from typing import List, Dict, Callable
import inflection

import firefly_integration.domain as domain


class Table:
    name: str = None
    path: str = None
    description: str = None
    columns: List[domain.Column] = []
    partitions: List[domain.Column] = []
    _partition_generators: Dict[str, Callable] = None
    duplicate_fields: List[str] = []
    duplicate_sort: List[str] = []
    database: domain.Database = None

    def __init__(self, name: str, columns: List[domain.Column], partitions: List[domain.Column] = None, path: str = '',
                 description: str = None, duplicate_fields: List[str] = None, duplicate_sort: List[str] = None,
                 partition_generators: Dict[str, Callable] = None):
        self.name = inflection.tableize(name)
        self.path = path
        self.columns = columns
        self.description = description
        self.partitions = partitions or []
        self.duplicate_fields = duplicate_fields or ['id']
        self.duplicate_sort = duplicate_sort or []
        self._partition_generators = partition_generators

        for column in self.columns:
            column.table = self

    def get_column(self, name: str):
        for column in self.columns:
            if column.name == name:
                return column
        raise domain.ColumnNotFound(name)

    def generate_partition_path(self, data: dict):
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

    def _pandas_type(self, t: type):
        if t is str:
            return 'string'
        if t is int:
            return 'integer'
        if t is float:
            return 'float'
        if t is bool:
            return 'boolean'
        if t is datetime:
            return 'timestamp'
        if t is date:
            return 'date'
