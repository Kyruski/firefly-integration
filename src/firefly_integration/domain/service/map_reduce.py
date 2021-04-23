from __future__ import annotations

from dataclasses import fields
from typing import Type

import firefly as ff
import pandas as pd
from moz_sql_parser import parse

OPS = {
    'eq': '==', 'ne': '!=', 'lt': '<', 'gt': '>', 'lte': '<=', 'gte': '>=', 'is': 'is'
}


class InvalidPartitions(Exception):
    pass


class MapReduce(ff.DomainService):
    _file_system: ff.FileSystem = None
    _batch_process: ff.BatchProcess = None
    _message_transport: ff.MessageTransport = None
    _message_factory: ff.MessageFactory = None
    _non_partition_keys: list = []
    _context: str = None

    def __call__(self, path: str, sql: str, entity: Type[ff.Entity], partitions: list = None):
        self._non_partition_keys = []
        query = parse(sql)
        fields_ = query['select']

        criteria = None
        if 'where' in query:
            criteria_dict = self._generate_criteria(query['where'])
            criteria = ff.BinaryOp.from_dict(criteria_dict)

        keys = self._generate_partition_paths(path, criteria, partitions)
        results = self._batch_process(self._list_partition, [(k,) for k in keys])
        keys = [k for sublist in results for k in sublist]

        without_partitions = criteria.prune(self._get_all_criteria_attributes(partitions, criteria))
        chunks = [keys[x:x+6] for x in range(0, len(keys), 6)]
        results = self._batch_process(self._invoke_mapper, [(chunk, fields_, without_partitions) for chunk in chunks])
        results = list(map(lambda r: pd.read_csv(r), results))

        ret = pd.concat(results)

        sort, unique = self._get_entity_order_and_unique_indexes(entity)
        ret.sort_values(sort, inplace=True)

        return ret[~ret.duplicated(subset=unique, keep='last')]

    def _invoke_mapper(self, keys: list, fields_: list, criteria: ff.BinaryOp):
        data = self._message_transport.request(
            self._message_factory.query(f'{self._context}.Map', criteria, {'keys': keys, 'fields': fields_})
        )
        return pd.read_csv(data)

    def _list_partition(self, path: str):
        return self._file_system.list(path)

    def _generate_partition_paths(self, path: str, criteria: ff.BinaryOp, partitions: list):
        paths = []
        if len(partitions) == 0:
            return [path]

        partition = partitions[0]
        ops = self._get_ops_for_attribute(partition, criteria)
        if len(ops) == 0:
            return [path]

        if partition == 'year':
            for op in ops:
                if op.op == '==':
                    val = op.lhv if isinstance(op.lhv, (str, int)) else op.rhv
                    paths.extend(self._generate_partition_paths(f'{path}/year={val}', criteria, partitions[1:]))
                else:
                    raise InvalidPartitions('Only equality operators are supported for partitions')

        elif partition == 'month':
            for op in ops:
                if op.op == '==':
                    val = op.lhv if isinstance(op.lhv, (str, int)) else op.rhv
                    paths.extend(self._generate_partition_paths(f'{path}/month={val}', criteria, partitions[1:]))
                else:
                    raise InvalidPartitions('Only equality operators are supported for partitions')

        return paths

    def _generate_criteria(self, data, criteria: dict = None):
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
                            'l': self._generate_criteria(value[0], criteria),
                            'o': op,
                            'r': self._generate_criteria(value[1], criteria),
                        }
                        for i in range(2, len(value)):
                            criteria = {
                                'l': criteria,
                                'o': op,
                                'r': self._generate_criteria(value[i], criteria)
                            }
                    else:
                        for i in range(len(value)):
                            criteria = {
                                'l': criteria,
                                'o': op,
                                'r': self._generate_criteria(value[i], criteria)
                            }

                elif op in ('eq', 'ne', 'lt', 'gt', 'lte', 'gte', 'is'):
                    return {
                        'l': self._generate_criteria(value[0], criteria),
                        'o': OPS[op],
                        'r': self._generate_criteria(value[1], criteria)
                    }

        return criteria

    def _is_in_where_clause(self, partition: str, criteria: ff.BinaryOp) -> bool:
        return criteria == criteria.prune([partition])

    def _get_ops_for_attribute(self, attribute: str, criteria: ff.BinaryOp):
        ret = []
        if (isinstance(criteria.lhv, ff.Attr) and str(criteria.lhv.attr) == attribute) or (
                isinstance(criteria.rhv, ff.Attr) and str(criteria.rhv.attr) == attribute):
            ret.append(criteria)
        if isinstance(criteria.lhv, ff.BinaryOp):
            ret.extend(self._get_ops_for_attribute(attribute, criteria.lhv))
        if isinstance(criteria.rhv, ff.BinaryOp):
            ret.extend(self._get_ops_for_attribute(attribute, criteria.rhv))

        return ret

    def _get_all_criteria_attributes(self, partitions: list, criteria: ff.BinaryOp):
        ret = []
        if isinstance(criteria.lhv, ff.Attr) and str(criteria.lhv.attr) not in partitions:
            ret.append(str(criteria.lhv.attr))
        if isinstance(criteria.rhv, ff.Attr) and str(criteria.rhv.attr) not in partitions:
            ret.append(str(criteria.rhv.attr))
        if isinstance(criteria.lhv, ff.BinaryOp):
            ret.extend(self._get_all_criteria_attributes(partitions, criteria.lhv))
        if isinstance(criteria.rhv, ff.BinaryOp):
            ret.extend(self._get_all_criteria_attributes(partitions, criteria.rhv))

        return ret

    def _get_entity_order_and_unique_indexes(self, entity: Type[ff.Entity]):
        unique = []
        sort = []

        # noinspection PyDataclass
        for field_ in fields(entity):
            if 'unique' in field_.metadata:
                unique.append((field_.name, field_.metadata['unique']))
            if 'sort' in field_.metadata:
                sort.append((field_.name, field_.metadata['sort']))

        unique.sort(key=lambda x: x[1])
        sort.sort(key=lambda x: x[1])

        return list(map(lambda x: x[0], sort)), list(map(lambda x: x[0], unique))
