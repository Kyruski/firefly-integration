from __future__ import annotations

from abc import ABC, abstractmethod

import firefly as ff
import pandas as pd

from ..data_catalog.table import Table


class Dal(ABC):
    @abstractmethod
    def store(self, data: pd.DataFrame, table: Table):
        pass

    @abstractmethod
    def load(self, table: Table, criteria: ff.BinaryOp = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def delete(self, criteria: ff.BinaryOp, table: Table):
        pass

