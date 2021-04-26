from __future__ import annotations

import awswrangler as wr
import firefly as ff
import firefly_integration.domain as domain
import pandas as pd
from firefly_integration.domain.service.dal import Dal


class AwsDal(Dal):
    def store(self, df: pd.DataFrame, table: domain.Table):
        wr.s3.to_parquet(
            df=df,
            path=table.generate_partition_path()
            compression='snappy',
            partition_cols=list(map(lambda c: c.name, table.partitions)),
            dtype=table.type_dict
        )

    def load(self, table: domain.Table, criteria: ff.BinaryOp = None) -> pd.DataFrame:
        pass

    def delete(self, criteria: ff.BinaryOp, table: domain.Table):
        pass
