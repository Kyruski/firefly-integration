from __future__ import annotations

from datetime import datetime, date
from typing import Union, List

import firefly as ff

import firefly_integration.domain as domain
import pandas as pd
import numpy as np


class SanitizeInputData(ff.DomainService):
    def __call__(self, data: Union[List[dict], dict], table: domain.Table) -> pd.DataFrame:
        df = pd.DataFrame(data if isinstance(data, list) else [data])
        for column in table.columns:
            if column.name not in df:
                if column.default is not domain.NoDefault:
                    df[column.name] = column.default
                elif column.required:
                    raise domain.InvalidInputData()
                else:
                    df[column.name] = None
            if column.data_type in (date, datetime):
                df[column.name] = pd.to_datetime(df[column.name])
            else:
                df[column.name] = df[column.name].astype(column.pandas_type)

        columns = list(map(lambda cc: cc.name, table.columns))
        for c in df.columns:
            if c not in columns:
                del df[c]

        df.replace({np.nan: None}, inplace=True)

        return df
