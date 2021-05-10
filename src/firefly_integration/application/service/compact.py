from __future__ import annotations

import firefly as ff
import firefly_integration.domain as domain


@ff.command_handler()
class Compact(ff.ApplicationService):
    _dal: domain.Dal = None
    _catalog_registry: domain.CatalogRegistry = None
    _context: str = None

    def __call__(self, table_name: str = None, path: str = None, **kwargs):
        # Run compaction on all tables
        if table_name is None:
            for table in self._catalog_registry.get_all_tables():
                self.invoke(f'{self._context}.Compact', {
                    'table_name': table.name,
                }, async_=True)

        # Run compaction on all partitions for the given table
        elif path is None:
            table = self._catalog_registry.get_table(table_name)
            for partition in self._dal.get_partitions(table):
                self.invoke(f'{self._context}.Compact', {
                    'table_name': table.name,
                    'path': partition,
                }, async_=True)

        # Run compaction on single partition
        else:
            table = self._catalog_registry.get_table(table_name)
            self._dal.compact(table=table, path=path)