from __future__ import annotations

import firefly as ff
import pandas as pd


@ff.query_handler()
class Map(ff.ApplicationService):
    _file_system: ff.FileSystem = None
    _batch_process: ff.BatchProcess = None
    _context: str = None
    _df: pd.DataFrame = None

    def __call__(self, keys: list, fields: list, criteria: dict):
        criteria = ff.BinaryOp.from_dict(criteria)
        results = self._batch_process(self._read, [(key[0], fields, criteria) for key in keys])

        return pd.concat(results).to_csv()

    def _read(self, key: str, fields: list, criteria: ff.BinaryOp):
        data = self._file_system.filter(key, fields, criteria)
        return pd.read_json(data)
