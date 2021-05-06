from __future__ import annotations

from typing import Type

import firefly as ff
import firefly_di as di

from .workflow_function import WorkflowFunction


class FunctionRegistry(ff.DomainService):
    _container: di.Container = None
    _functions: dict = {}

    def register(self, key: str, function: Type[WorkflowFunction]):
        self._functions[key] = self._container.build(function)

    def get(self, key: str) -> WorkflowFunction:
        return self._functions[key]
