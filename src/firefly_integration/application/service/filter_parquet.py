from __future__ import annotations

import firefly as ff
import pandas as pd

import firefly_integration.domain as domain


@ff.command_handler()
class FilterParquet(ff.ApplicationService):
    _catalog_registry: domain.CatalogRegistry = None
    _sanitize_input_data: domain.SanitizeInputData = None
    _filter_parquet: domain.FilterParquet = None
    _batch_process: ff.BatchProcess = None
    _dal: domain.Dal = None

    def __call__(self, files: list, fields: list, criteria: dict, result_file: str, table_name: str, **kwargs):
        criteria = ff.BinaryOp.from_dict(criteria)
        results = self._batch_process(self._do_filter, [(file, fields, criteria) for file in files])
        df = pd.concat(results)
        table = self._catalog_registry.get_table(table_name)
        df = self._sanitize_input_data(df, table)
        self._dal.write_tmp_file(result_file, df)

    def _do_filter(self, path: str, fields: list, criteria: ff.BinaryOp):
        return self._filter_parquet(path, fields, criteria)
