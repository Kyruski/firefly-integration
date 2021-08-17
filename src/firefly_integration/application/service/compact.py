from __future__ import annotations

import awswrangler as wr
import firefly as ff

import firefly_integration.domain as domain


@ff.command_handler()
class Compact(ff.ApplicationService):
    _dal: domain.Dal = None
    _catalog_registry: domain.CatalogRegistry = None
    _context: str = None

    def __call__(self, table_name: str = None, path: str = None, **kwargs):
        # Run compaction on all tables / partitions
        if table_name is None:
            for table in self._catalog_registry.get_all_tables():
                if len(table.partitions) > 0 or table.time_partitioning is not None:
                    self._scan_partitions(table)
                else:
                    self.invoke(f'{self._context}.Compact', {
                        'table_name': table.name,
                        'path': table.full_path(),
                    }, async_=True)

        # Run compaction on single partition
        elif path is not None:
            table = self._catalog_registry.get_table(table_name)
            self._dal.compact(table=table, path=path)

    def _scan_partitions(self, table: domain.Table):
        files = wr.s3.list_objects('s3://' + table.full_path())
        partitions = []
        for file in files:
            if file.endswith('.dat.snappy.parquet'):
                continue
            partition = '/'.join(file.split('/')[:-1])
            if partition not in partitions:
                partitions.append(partition)
                self.invoke(f'{self._context}.Compact', {
                    'table_name': table.name,
                    'path': partition,
                }, async_=True)
