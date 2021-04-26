from __future__ import annotations

from io import StringIO

import firefly as ff
import pandas as pd


@ff.query_handler()
class Map(ff.ApplicationService):
    _file_system: ff.FileSystem = None
    _batch_process: ff.BatchProcess = None
    _context: str = None
    _df: pd.DataFrame = None

    def __call__(self, keys: list, fields: list, criteria: dict, types: dict):
        criteria = ff.BinaryOp.from_dict(criteria)
        results = self._batch_process(self._read, [(key[0], fields, criteria, types) for key in keys])

        return pd.concat(results).to_json()

    def _read(self, key: str, fields: list, criteria: ff.BinaryOp, types: dict):
        data = self._file_system.filter(key, fields, criteria)
        ret = pd.read_json(StringIO(data), dtype=types)
        # ret.set_index(['id'], inplace=True)

        return ret
