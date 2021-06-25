from __future__ import annotations

from time import sleep

import firefly as ff
from botocore.exceptions import ClientError

import firefly_integration.domain as domain


@ff.command_handler()
class DeduplicatePartitions(ff.ApplicationService):
    _dal: domain.Dal = None
    _catalog_registry: domain.CatalogRegistry = None
    _context: str = None

    def __call__(self, table_name: str = None, path: str = None, **kwargs):
        # Run deduplication on all tables
        if table_name is None:
            for table in self._catalog_registry.get_all_tables():
                self.invoke(f'{self._context}.DeduplicatePartitions', {
                    'table_name': table.name,
                }, async_=True)

        # Run deduplication on all partitions for the given table
        elif path is None:
            table = self._catalog_registry.get_table(table_name)
            if table is not None and len(table.partitions) > 0 or table.time_partitioning is not None:
                try:
                    counter = 0
                    for partition in self._dal.get_partitions(table):
                        self.invoke(f'{self._context}.DeduplicatePartitions', {
                            'table_name': table.name,
                            'path': partition,
                        }, async_=True)
                        counter += 1
                        if counter % 5 == 0:
                            sleep(1)  # Try to avoid unnecessarily hitting the Athena query rate limit.
                except ClientError as e:
                    self.info(str(e))
            elif table is not None:
                self.invoke(f'{self._context}.DeduplicatePartitions', {
                    'table_name': table.name,
                    'path': table.full_path(),
                }, async_=True)

        # Run deduplication on single partition
        else:
            table = self._catalog_registry.get_table(table_name)
            self._dal.deduplicate_partition(table=table, path=path)
