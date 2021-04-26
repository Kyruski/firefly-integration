from __future__ import annotations

from typing import List

import firefly_integration.domain as domain


class Catalog:
    databases: List[domain.Database] = []

    def __init__(self, databases: List[domain.Database]):
        self.databases = databases

    def get_database(self, name: str = None):
        if name is None and len(self.databases) == 1:
            return self.databases[0]
        ret = list(filter(lambda x: x.name == name, self.databases))

        return None if len(ret) == 0 else ret[0]
