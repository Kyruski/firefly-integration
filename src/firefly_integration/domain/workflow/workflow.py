from __future__ import annotations

import graphviz


class Workflow:
    name: str = None

    def __init__(self, name: str = None, module: str = None):
        self.name = name
        self.module = module
        self._nodes = {}

    def step(self, name: str, label: str, dependencies: list = None, url: str = None) -> Dag:
        self._nodes[name] = {
            'dependencies': dependencies or [],
            'label': label,
            'url': url,
        }
        return self

    def batch(self):
        ret = []
        complete = []
        incomplete = list(self._nodes.keys())

        while True:
            batch = []
            for k, v in self._nodes.items():
                if all(e in complete for e in v['dependencies']):
                    if k not in complete:
                        batch.append(k)

            ret.append(batch)
            for item in batch:
                complete.append(item)
                incomplete.remove(item)

            if len(incomplete) == 0:
                break

        return ret

    def render(self, file: str):
        dot = graphviz.Digraph(comment='Pre-Processing of Workouts')

        for k, v in self._nodes.items():
            attrs = {}
            if v['url'] is not None:
                attrs['URL'] = v['url']

            dot.node(k, v['label'], **attrs)
            for dependency in v['dependencies']:
                dot.edge(dependency, k)

        dot.render(file)
