from __future__ import annotations

import inspect

import firefly_di as di

import firefly_integration.infrastructure.service.dal as dal
from firefly_integration.domain.service.dal import Dal

dal_class = None
for k, v in dal.__dict__.items():
    if inspect.isclass(v) and issubclass(v, Dal):
        dal_class = v
        break


class Container(di.Container):
    dal: Dal = dal_class
