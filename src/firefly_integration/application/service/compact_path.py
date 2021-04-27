from __future__ import annotations

import firefly as ff
import firefly_integration.domain as domain


@ff.command_handler()
class CompactPath(ff.ApplicationService):
    _compact_path: domain.CompactPath = None

    def __call__(self, path: str, type_dict: dict, sort_fields: list, unique_fields: list, **kwargs):
        self._compact_path(path, type_dict, sort_fields, unique_fields)
