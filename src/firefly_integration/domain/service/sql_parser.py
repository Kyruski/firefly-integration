from __future__ import annotations

from typing import Tuple, List

import firefly as ff
from moz_sql_parser import parse

import firefly_integration.domain as domain

OPS = {
    'eq': '==', 'ne': '!=', 'lt': '<', 'gt': '>', 'lte': '<=', 'gte': '>=', 'is': 'is'
}


class SqlParser(ff.DomainService):
    _parts: dict = None
    _non_partition_keys: list = None

    def parse(self, sql: str):
        self._parts = parse(sql)
        self._non_partition_keys = []

    def get_select_fields(self):
        self._ensure_parse_called()
        return self._parts['select']

    def get_table(self):
        self._ensure_parse_called()
        if isinstance(self._parts['from'], str):
            return self._parts['from']
        for table in self._parts['from']:
            if 'name' in table and table['name'] == 'x':
                return table['value']

    def get_sort_order(self) -> Tuple[List[str], List[bool]]:
        self._ensure_parse_called()
        fields = []
        ascending = []
        if 'orderby' in self._parts:
            order_parts = self._parts['orderby']
            if not isinstance(order_parts, list):
                order_parts = [order_parts]
            for part in order_parts:
                fields.append(part['value'])
                if 'sort' in part:
                    ascending.append(part['sort'] == 'asc')
                else:
                    ascending.append(True)
        else:
            fields.append('id')
            ascending.append(True)

        return fields, ascending

    def get_criteria(self, data=None, criteria: dict = None):
        self._ensure_parse_called()

        if data is None:
            if 'where' not in self._parts:
                return
            data = self._parts['where']

        if isinstance(data, str):
            self._non_partition_keys.append(data)
            return f'a:{data}'
        elif isinstance(data, (int, float, bool)):
            return data
        elif isinstance(data, dict) and 'literal' in data:
            return data['literal']

        if isinstance(data, dict):
            for op, value in data.items():
                if op in ('and', 'or'):
                    if criteria is None:
                        criteria = {
                            'l': self.get_criteria(value[0], criteria),
                            'o': op,
                            'r': self.get_criteria(value[1], criteria),
                        }
                        for i in range(2, len(value)):
                            criteria = {
                                'l': criteria,
                                'o': op,
                                'r': self.get_criteria(value[i], criteria)
                            }
                    else:
                        for i in range(len(value)):
                            criteria = {
                                'l': criteria,
                                'o': op,
                                'r': self.get_criteria(value[i], criteria)
                            }

                elif op in ('eq', 'ne', 'lt', 'gt', 'lte', 'gte', 'is'):
                    return {
                        'l': self.get_criteria(value[0], criteria),
                        'o': OPS[op],
                        'r': self.get_criteria(value[1], criteria)
                    }

        return criteria

    def get_all_criteria_attributes(self, partitions: list, criteria: ff.BinaryOp):
        ret = []
        if isinstance(criteria.lhv, ff.Attr) and str(criteria.lhv.attr) not in partitions:
            ret.append(str(criteria.lhv.attr))
        if isinstance(criteria.rhv, ff.Attr) and str(criteria.rhv.attr) not in partitions:
            ret.append(str(criteria.rhv.attr))
        if isinstance(criteria.lhv, ff.BinaryOp):
            ret.extend(self.get_all_criteria_attributes(partitions, criteria.lhv))
        if isinstance(criteria.rhv, ff.BinaryOp):
            ret.extend(self.get_all_criteria_attributes(partitions, criteria.rhv))

        return ret

    def _ensure_parse_called(self):
        if self._parts is None:
            raise domain.IntegrationError('You must call parse() before get_criteria()')
