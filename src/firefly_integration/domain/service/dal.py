from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

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

    @abstractmethod
    def get_partitions(self, table: Table, criteria: ff.BinaryOp = None) -> List[str]:
        pass

    @abstractmethod
    def wait_for_tmp_files(self, files: list):
        pass

    @abstractmethod
    def read_tmp_files(self, files: list) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_tmp_file(self, file: str, data: pd.DataFrame):
        pass

    @abstractmethod
    def compact(self, table: Table, path: str):
        pass

    @abstractmethod
    def deduplicate_partition(self, table: Table, path: str):
        pass
