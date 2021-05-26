from __future__ import annotations

import firefly as ff
import firefly_integration.domain as ffi


@ff.command_handler()
class StoreData(ff.ApplicationService):
    _catalog_registry: ffi.CatalogRegistry = None
    _store_data: ffi.StoreData = None

    def __call__(self, data, table: str, **kwargs):
        return self._store_data(data, self._catalog_registry.get_table(table))
