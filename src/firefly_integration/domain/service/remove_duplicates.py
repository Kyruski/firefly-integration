from __future__ import annotations

import firefly as ff
import pandas as pd

import firefly_integration.domain as domain


class RemoveDuplicates(ff.DomainService):
    def __call__(self, df: pd.DataFrame, table: domain.Table):
        df.sort_values(table.duplicate_sort, inplace=True)
        df.drop_duplicates(subset=table.duplicate_fields, keep='last', inplace=True)
        if 'id' in df:
            df.set_index(['id'], inplace=True)