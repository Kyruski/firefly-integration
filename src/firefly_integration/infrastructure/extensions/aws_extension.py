from __future__ import annotations

import importlib
import inspect
from datetime import datetime, date

import firefly as ff
import inflection
from troposphere import Template
from troposphere.glue import Database, DatabaseInput, Table, TableInput, Column, StorageDescriptor, SerdeInfo

import firefly_integration.domain as ffi


@ff.agent.pre_deploy_hook(for_='aws')
class AddGlueDataCatalog(ff.AgentExtension):
    _account_id: str = None

    def __call__(self, template: Template, context: ff.Context, env: str, **kwargs):
        for catalog in self._load_catalogs(context):
            for database in catalog.databases:
                self._add_database(template, database)

    def _add_database(self, template: Template, database: ffi.Database):
        db = template.add_resource(Database(
            inflection.camelize(database.name, uppercase_first_letter=True),
            CatalogId=self._account_id,
            DatabaseInput=DatabaseInput(
                Description=database.description or '',
                Name=database.name
            )
        ))

        path = database.path.rstrip('/')
        if not path.startswith('s3://'):
            path = f's3://{path.lstrip("/")}'
        for table in database.tables:
            self._add_table(template, table, database.name, db, path)

    def _add_table(self, template: Template, table: ffi.Table, db_name: str, db, prefix: str):
        path = f'{prefix}/{table.path.strip("/")}'
        path = f'{path.rstrip("/")}/{table.name}'

        template.add_resource(Table(
            inflection.camelize(table.name, uppercase_first_letter=True),
            CatalogId=self._account_id,
            DatabaseName=db_name,
            TableInput=TableInput(
                Description=table.description or '',
                Name=table.name,
                Parameters={
                    'projection.enabled': False,
                },
                PartitionKeys=[
                    Column(Comment=part.comment or '', Name=part.name, Type=self._athena_type(part.data_type))
                    for part in table.partitions
                ],
                StorageDescriptor=StorageDescriptor(
                    InputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    OutputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    SerdeInfo=SerdeInfo(
                        SerializationLibrary='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    ),
                    Location=path,
                    Columns=[
                        Column(Comment=c.comment or '', Name=c.name, Type=self._athena_type(c.data_type))
                        for c in table.columns
                    ]
                )
            ),
            DependsOn=[db]
        ))

    def _athena_type(self, t: type):
        if t is str:
            return 'string'
        if t is int:
            return 'integer'
        if t is float:
            return 'float'
        if t is bool:
            return 'boolean'
        if t is datetime:
            return 'timestamp'
        if t is date:
            return 'date'

    def _load_catalogs(self, context: ff.Context):
        module_name = context.config.get('domain_module', '{}.domain')
        try:
            module = importlib.import_module(module_name.format(context.name))
        except (ModuleNotFoundError, KeyError):
            return []

        ret = []
        for k, v in module.__dict__.items():
            if isinstance(v, ffi.Catalog):
                ret.append(v)

        return ret
