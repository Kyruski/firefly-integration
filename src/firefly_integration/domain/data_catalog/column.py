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
    comment: str = None
    required: bool = False
    default = NoDefault
    table: domain.Table = None

    def __init__(self, name: str, data_type: type, comment: str = None, required: bool = False, default=NoDefault):
        if data_type not in ALLOWED_TYPES:
            raise domain.InvalidDataType(
                f'Data type {data_type} is not valid for {name}. Allowed types are: {ALLOWED_TYPES}'
            )

        self.name = name
        self.data_type = data_type
        self.comment = comment
        self.required = required
        self.default = default

    def set_type(self, df: pd.DataFrame):
        df[self.name].astype(str(self.data_type), inplace=True)
        return df
