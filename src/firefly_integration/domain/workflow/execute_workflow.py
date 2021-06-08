from __future__ import annotations

import importlib
import inspect

import firefly as ff
import inflection

from .function_parameters import FunctionParameters
from .function_registry import FunctionRegistry
from .workflow import Workflow
from .workflow_function import WorkflowFunction


class ExecuteWorkflow(ff.DomainService):
    _batch_process: ff.BatchProcess = None
    _function_registry: FunctionRegistry = None
    _loaded: list = []

    def __call__(self, workflow: Workflow, data: FunctionParameters):
        if workflow not in self._loaded:
            self._load_workflow_functions(workflow)
            self._loaded.append(workflow)

        for batch in workflow.batch():
            for function in batch:
                data = self._execute_function(function, data)

        return data

    def _execute_function(self, function: str, data: FunctionParameters):
        return self._function_registry.get(function)(data)

    def _load_workflow_functions(self, workflow: Workflow):
        module = importlib.import_module(workflow.module)
        for k, v in module.__dict__.items():
            if inspect.isclass(v) and issubclass(v, WorkflowFunction):
                self._function_registry.register(inflection.underscore(v.__name__), v)
