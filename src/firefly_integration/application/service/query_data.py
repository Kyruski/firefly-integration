from __future__ import annotations

import firefly as ff
import firefly_integration.domain as ffi


@ff.query_handler()
class QueryData(ff.ApplicationService):
    _catalog_registry: ffi.CatalogRegistry = None
    _query_warehouse: ffi.QueryWarehouse = None

    def __call__(self, sql: str, **kwargs):
        return self._query_warehouse(sql)
