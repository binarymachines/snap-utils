#!/usr/bin/env python


import os

import sqldbx
import couchbasedbx as cbx
import redisx
import telegraf

from snap import snap
from snap import common
import constants as const

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy_utils import UUIDType
import uuid

Base = declarative_base()


class PostgresServiceObject(object):
    def __init__(self, **kwargs):
        self.log = logger
        self.host = kwargs['host']
        self.port = int(kwargs.get('port', 5432))
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        self.db_name = kwargs['database']
        self.db = sqldbx.PostgreSQLDatabase(self.host, self.db_name, self.port)
        self.db.login(self.username, kwargs['password'])
        self._data_manager = sqldbx.PersistenceManager(self.db)


    @property
    def data_manager(self):
        return self._data_manager

    @property
    def database(self):
        return self.db


class MSSQLServiceObject(object):
    def __init__(self, logger, **kwargs):
        kwreader = common.KeywordArgReader('host', 'username', 'database', 'password')
        kwreader.read(**kwargs)

        self.log = logger
        self.host = kwreader.get_value('host')
        self.port = int(kwreader.get_value('port') or 1433)
        self.username = kwreader.get_value('username')
        self.db_name = kwreader.get_value('database')
        self.password = kwreader.get_value('password')
        self.db = sqldbx.SQLServerDatabase(self.host, self.db_name, self.port)
        self.db.login(self.username, self.password)
        self._data_manager = sqldbx.PersistenceManager(self.db)


    @property
    def data_manager(self):
        return self._data_manager

    @property
    def database(self):
        return self.db


class RedshiftServiceObject():
    def __init__(self, logger, **kwargs):
        logger.info('>>> initializing RedshiftServiceObject with params: %s' % (kwargs))

        self.host = kwargs['host']
        self.db_name = kwargs['db_name']
        self.port = kwargs['port']
        self.username = kwargs['username']
        self.schema = kwargs['schema']
        self.data_manager = None
        self.db = sqldbx.PostgreSQLDatabase(self.host, self.db_name, self.port)


    def login(self, password):
        self.db.login(self.username, password, self.schema)
        self.data_manager = sqldbx.PersistenceManager(self.db)


    def get_connection(self):
        return self.db.engine.connect()



class CouchbaseServiceObject():
    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.data_bucket_name = kwargs['data_bucket_name']
        self.journal_bucket_name = kwargs['journal_bucket_name']
        self.cache_bucket_name = kwargs['cache_bucket_name']

        self.couchbase_server = cbx.CouchbaseServer(self.host)
        self.data_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.data_bucket_name)
        self.journal_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.journal_bucket_name)
        self.cache_manager = cbx.CouchbasePersistenceManager(self.couchbase_server, self.cache_bucket_name)


    def insert_record(self, record_type_name, record_dict):
        cb_record = cbx.CouchbaseRecordBuilder(record_type_name).add_fields(record_dict).build()
        return self.data_manager.insert_record(cb_record)


class RedisServiceObject():
    def __init__(self, **kwargs):        
        self.host = kwargs['host']
        self.port = kwargs.get('port', 6379)
        self.redis_server = redisx.RedisServer(self.host, self.port)
        self.transformed_record_queue_name = kwargs['transformed_record_queue_name']
        self.raw_record_queue_name = kwargs['raw_record_queue_name']
        self.generator_to_user_map_name = kwargs['generator_user_map_name']


    def get_transformed_record_queue(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.transformed_record_queue_name)
        self.log.info('generated redis key for transformed record queue: "%s"' % key)
        return redisx.Queue(key, self.redis_server)


    def get_raw_record_queue(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.raw_record_queue_name)
        self.log.info('generated redis key for raw record queue: "%s"' % key)
        return redisx.Queue(key, self.redis_server)


    def get_generator_to_user_map(self, pipeline_id):
        key = redisx.compose_key(pipeline_id, self.generator_to_user_map_name)
        self.log.info('generated redis key for generator-to-user map: "%s"' % key)
        return redisx.Hashtable(key, self.redis_server)
