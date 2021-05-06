from __future__ import annotations

from abc import ABC, abstractmethod

import firefly as ff

from .function_parameters import FunctionParameters


class WorkflowFunction(ff.LoggerAware, ff.SystemBusAware, ABC):
    @abstractmethod
    def __call__(self, p: FunctionParameters) -> FunctionParameters:
        pass
