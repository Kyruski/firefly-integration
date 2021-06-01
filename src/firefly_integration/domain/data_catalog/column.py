from __future__ import annotations

from datetime import datetime, date

import pandas as pd

import firefly_integration.domain as domain

ALLOWED_TYPES = (str, int, float, bool, datetime, date)


class NoDefault:
    pass


class Column:
    name: str = None
    data_type: type = None
    description: str = None
    required: bool = False
    default = NoDefault
    meta: dict = {}
    table: domain.Table = None

    def __init__(self, name: str, data_type: type, description: str = None, required: bool = False, default=NoDefault,
                 meta: dict = None):
        if data_type not in ALLOWED_TYPES:
            raise domain.InvalidDataType(
                f'Data type {data_type} is not valid for {name}. Allowed types are: {ALLOWED_TYPES}'
            )

        self.name = name
        self.data_type = data_type
        self.description = description
        self.required = required
        self.default = default
        self.meta = meta or {}

    def set_type(self, df: pd.DataFrame):
        df[self.name].astype(str(self.data_type), inplace=True)
        return df

    @property
    def pandas_type(self):
        if self.data_type is int:
            return 'Int64'
        elif self.data_type is str:
            return 'string'
        else:
            return self.data_type

