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
        self._dal.store(df, table)
