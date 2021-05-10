from __future__ import annotations

from datetime import datetime, date
from typing import Union, List

import firefly as ff

import firefly_integration.domain as domain
import pandas as pd
import numpy as np


class MarshalDataframe(ff.DomainService):
    def __call__(self, df: pd.DataFrame, table: domain.Table) -> pd.DataFrame:
        for column in table.columns:
            if column.data_type in (date, datetime) and column.name in df:
                df[column.name] = pd.to_datetime(df[column.name])

        return df
