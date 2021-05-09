from __future__ import annotations

import importlib

import firefly as ff

import firefly_integration.domain as ffi


@ff.on('firefly.ApplicationLayerLoaded')
class LoadDataCatalogs(ff.ApplicationService):
    _context_map: ff.ContextMap = None
    _catalog_registry: ffi.CatalogRegistry = None

    def __call__(self, **kwargs):
        for context in self._context_map.contexts:
            if context.name != 'firefly' and not context.name.startswith('firefly_'):
                self._load_data_catalogs(context)

    def _load_data_catalogs(self, context: ff.Context):
        module_name = f'{context.name}.domain'
        try:
            module = importlib.import_module(module_name.format(context.name))
        except (ModuleNotFoundError, KeyError):
            return []

        for k, v in module.__dict__.items():
            if isinstance(v, ffi.Catalog):
                self._catalog_registry.add_catalog(v)
