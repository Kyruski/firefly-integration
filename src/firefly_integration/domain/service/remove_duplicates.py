from __future__ import annotations

import firefly as ff
import pandas as pd


class RemoveDuplicates(ff.DomainService):
    def __call__(self, df: pd.DataFrame, sort_fields: list, unique_fields: list):
        df.sort_values(sort_fields, inplace=True)
        df.drop_duplicates(subset=unique_fields, keep='last', inplace=True)
        df.set_index(['id'], inplace=True)
