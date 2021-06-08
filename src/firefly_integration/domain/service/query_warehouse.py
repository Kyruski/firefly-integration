from __future__ import annotations

import multiprocessing
import uuid
from typing import Optional

import firefly as ff
import pandas as pd

import firefly_integration.domain as domain
import awswrangler as wr


class QueryWarehouse(ff.DomainService):
    _catalog_registry: domain.CatalogRegistry = None
    _sql_parser: domain.SqlParser = None
    _batch_process: ff.BatchProcess = None
    _filter_parquet: FilterParquet = None
    _remove_duplicates: domain.RemoveDuplicates = None
    _dal: domain.Dal = None
    _file_system: ff.FileSystem = None
    _ff_environment: str = None

    def __init__(self):
        self._cpu_count = multiprocessing.cpu_count()
        self._threshold = self._cpu_count

    def __call__(self, sql: str, table: domain.Table = None) -> Optional[pd.DataFrame]:
        # This is temporary code to get warehouse queries working. This uses athena. We either need to move this
        # code, specifically the aws wrangler part, to an infrastructure class or finish the original approach using
        # lambda. Also, the database name is assumed here, and it shouldn't be.
        self._sql_parser.parse(sql)
        if table is None:
            table: domain.Table = self._catalog_registry.get_table(self._sql_parser.get_table())

        results = wr.athena.read_sql_query(
            sql=sql,
            database=f'data_warehouse_{self._ff_environment}',
            ctas_approach=False,
            use_threads=True
        )

        try:
            self._remove_duplicates(results, table)
            self._sort(results)
        except KeyError:
            pass

        return results

        # partition_criteria, select_criteria = self._process_criteria(table)
        # paths = self._dal.get_partitions(table, partition_criteria)
        # if len(paths) == 0:
        #     return None
        #
        # files = self._batch_process(self._list_files, [(path,) for path in paths])
        # files = [item for sb in files for item in sb]
        #
        # fields = self._sql_parser.get_select_fields()
        # if len(files) <= self._threshold:
        #     results = self._batch_process(self._filter_parquet, [(file, fields, select_criteria) for file in files])
        #     results = pd.concat(results)
        # else:
        #     results = self._fan_out(files, fields, select_criteria, table)
        #
        # self._remove_duplicates(results, table)
        # self._sort(results)
        #
        # return results

    def _fan_out(self, files: list, fields: list, select_criteria: ff.BinaryOp, table: domain.Table):
        output_path = f'tmp/ff-query-results/{str(uuid.uuid4())}'
        output_files = []
        for batch in ff.chunk(files, self._threshold):
            output_files.append(f'{output_path}/{str(uuid.uuid4())}.snappy.parquet')
            self.invoke('integration.FilterParquet', {
                'files': batch,
                'fields': fields,
                'criteria': select_criteria.to_dict() if select_criteria is not None else None,
                'result_file': output_files[-1],
                'table_name': table.name,
            }, async_=True)

        return self._wait_for_results(output_files)

    def _process_criteria(self, table: domain.Table):
        criteria_dict = self._sql_parser.get_criteria()
        partition_criteria = None
        select_criteria = None
        if criteria_dict is not None:
            criteria = ff.BinaryOp.from_dict(criteria_dict)
            partitions = list(map(lambda t: t.name, table.partitions))
            partition_criteria = criteria.prune(partitions)
            select_criteria = criteria.prune(
                self._sql_parser.get_all_criteria_attributes(partitions, criteria)
            )
        return partition_criteria, select_criteria

    def _list_files(self, path: str):
        return list(map(lambda f: f[0], self._file_system.list(path)))

    def _wait_for_results(self, files: list):
        self._dal.wait_for_tmp_files(files)

        return self._dal.read_tmp_files(files)

    def _sort(self, data: pd.DataFrame):
        fields, ascending = self._sql_parser.get_sort_order()
        if len(list(map(lambda x: x not in data, fields))) == 0:
            data.sort_values(by=fields, ascending=ascending, inplace=True)


class FilterParquet(ff.DomainService):
    _file_system: ff.FileSystem = None
    _serializer: ff.Serializer = None

    def __call__(self, path: str, fields: list, criteria: ff.BinaryOp):
        return pd.DataFrame(self._serializer.deserialize(
            self._file_system.filter(path.lstrip('s3://'), fields, criteria)
        ))
