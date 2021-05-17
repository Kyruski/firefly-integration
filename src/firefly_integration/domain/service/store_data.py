from __future__ import annotations

from datetime import datetime, date
from typing import Union, List

import firefly as ff

import firefly_integration.domain as domain
import pandas as pd
import numpy as np


class StoreData(ff.DomainService):
    _sanitize_input_data: domain.SanitizeInputData = None
    _dal: domain.Dal = None

    def __call__(self, data, table: domain.Table):
        df = self._sanitize_input_data(data, table)

        if table.time_partitioning and table.time_partitioning_column:
            df['year'] = pd.DatetimeIndex(df[table.time_partitioning_column]).year
            if table.time_partitioning in ('month', 'day'):
                df['month'] = pd.DatetimeIndex(df[table.time_partitioning_column]).month
            if table.time_partitioning == 'day':
                df['day'] = pd.DatetimeIndex(df[table.time_partitioning_column]).day

        self._dal.store(df, table)
