from __future__ import annotations

from typing import List

import firefly_integration.domain as domain


class Database:
    name: str = None
    path: str = None
    description: str = None
    tables: List[domain.Table] = []

    def __init__(self, name: str, path: str, tables: List[domain.Table], description: str = None):
        self.name = name
        self.path = path
        self.tables = tables
        self.description = description

        for table in self.tables:
            table.database = self

    def get_table(self, name: str):
        ret = list(filter(lambda x: x.name == name, self.tables))

        return None if len(ret) == 0 else ret[0]
