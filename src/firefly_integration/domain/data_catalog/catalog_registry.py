from __future__ import annotations

from typing import List, Optional

import firefly as ff
import firefly_integration.domain as domain


class CatalogRegistry(ff.DomainService):
    _catalogs: List[domain.Catalog] = []

    def add_catalog(self, catalog: domain.Catalog):
        self._catalogs.append(catalog)

    def get_all_tables(self) -> List[domain.Table]:
        ret = []
        for catalog in self._catalogs:
            for database in catalog.databases:
                for table in database.tables:
                    ret.append(table)

        return ret

    def get_table(self, table_name: str) -> Optional[domain.Table]:
        for catalog in self._catalogs:
            for database in catalog.databases:
                for table in database.tables:
                    if table.name == table_name:
                        return table
