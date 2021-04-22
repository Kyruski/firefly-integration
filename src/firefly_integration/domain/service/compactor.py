from __future__ import annotations

import bz2
import gzip
from abc import ABC
from io import StringIO
from typing import Callable

import pandas as pd

import firefly as ff

JSON = 'json'
CSV = 'csv'
PARQUET = 'parquet'
FILE_TYPES = (JSON, CSV, PARQUET)

BZIP2 = 'bzip2'
GZIP = 'gzip'
COMPRESSION_TYPES = (BZIP2, GZIP)


class Compactor(ABC):
    def __init__(self, input_format: str, output_format: str, input_compression: str = None,
                 output_compression: str = None):
        assert input_format in FILE_TYPES
        assert output_format in FILE_TYPES
        assert input_compression in COMPRESSION_TYPES
        assert output_compression in COMPRESSION_TYPES

        self._input_format = input_format
        self._output_format = output_format
        self._input_compression = input_compression
        self._output_compression = output_compression

    def __call__(self, *args, **kwargs):
        pass

    def _decompress_bzip2(self, data, next_: Callable):
        return next_(bz2.decompress(data))

    def _decompress_gzip(self, data, next_: Callable):
        return next_(gzip.decompress(data))

    def _parse_json(self, data, next_: Callable):
        return next_(pd.read_json(StringIO(data)))

    def _parse_csv(self, data, next_: Callable):
        return next_(pd.read_csv(StringIO(data)))
