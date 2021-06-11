from __future__ import annotations

from datetime import datetime, date
from typing import Union, List

import firefly as ff

import firefly_integration.domain as domain
import pandas as pd
import numpy as np


class SanitizeInputData(ff.DomainService):
    def __call__(self, data: Union[List[dict], dict, pd.DataFrame], table: domain.Table,
                 validate: bool = True) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            df = pd.DataFrame(data if isinstance(data, list) else [data])
        else:
            df = data

        for column in table.columns:
            if column.name not in df:
                if column.default is not domain.NoDefault:
                    df[column.name] = column.default
                elif column.required and validate is True:
                    raise domain.InvalidInputData()
                elif df.index.name != column.name:
                    df[column.name] = np.nan
            if column.data_type in (date, datetime):
                if df[column.name].dtype == 'object':
                    try:
                        df[column.name] = np.float64(df[column.name])
                    except ValueError:
                        pass
                df[column.name] = pd.to_datetime(df[column.name])
            elif df.index.name != column.name:
                if column.data_type is int or column.data_type == 'int':
                    try:
                        df[column.name] = df[column.name].astype(np.float64).astype(np.int64)
                    except ValueError:
                        df[column.name] = np.nan
                else:
                    df[column.name] = df[column.name].astype(column.pandas_type)

        columns = list(map(lambda cc: cc.name, table.columns))
        for c in df.columns:
            if c not in columns:
                try:
                    del df[c]
                except KeyError:
                    pass

        df.replace({np.nan: None}, inplace=True)

        return df
