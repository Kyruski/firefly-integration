from __future__ import annotations

import firefly as ff
import pandas as pd

import firefly_integration.domain as ffi


@ff.query_handler()
class QueryData(ff.ApplicationService):
    _catalog_registry: ffi.CatalogRegistry = None
    _query_warehouse: ffi.QueryWarehouse = None

    def __call__(self, sql: str, **kwargs):
        ret: pd.DataFrame = self._query_warehouse(sql)

        return ret.to_json(orient='records')
