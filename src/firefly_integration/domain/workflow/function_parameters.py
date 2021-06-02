from __future__ import annotations

from abc import ABC, abstractmethod


class FunctionParameters(ABC):
    @abstractmethod
    def debug(self) -> str:
        pass
