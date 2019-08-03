import copy
from subprocess import call

from django.db import connections


class MultiTenantProvisioner(object):
    def __init__(self, super_user, db_name, users=[], create_db=False):
        self.super_user = super_user
        self.db_name = db_name
        self.users = users
        self.create_db = create_db

    def execute_many_django(self, using, queries):
        with connections[using].cursor() as cursor:
            for query, params in self.process_queries(queries):
                cursor.execute(query, params)

    def execute_many_subprocess(self, using, queries):
        new_queries = [query % params for query, params in self.process_queries(queries)]
        call('psql', '-d %s' % using, '-c %s' % '\n'.join(new_queries))

    def process_queries(self, queries):
        new_queries = []
        for query in queries:
            if len(query) == 1:
                params = []
            else:
                query, params = query
            if not query.endswith(';'):
                query += ';'
            new_queries.append((query, params))
        return new_queries

    def execute_many(self, using, queries):
        self.execute_many_subprocess(using, queries)

    def execute(self, using, query):
        self.execute_many(using, [query])

    def reset(self):
        self.execute(self.db_name, ('drop schema %s', [self.super_user]))
        queries = [
            ('drop role %s_users', [self.super_user])
        ]
        for user in self.users:
            queries.append('drop role %s', (user,))
        self.execute_many(self.db_name, queries)

    def harden_default_perms(self):
        self.execute_many('template1', [
            'REVOKE ALL ON DATABASE template1 FROM public',
            'REVOKE ALL ON SCHEMA public FROM public',
            'GRANT ALL ON SCHEMA public TO postgres',
            ('GRANT ALL ON SCHEMA public TO %s', [self.super_user]),
        ])
        self.execute_many('template1', [
            'REVOKE ALL ON pg_user FROM public'
            'REVOKE ALL ON pg_roles FROM public'
            'REVOKE ALL ON pg_group FROM public'
            'REVOKE ALL ON pg_authid FROM public'
            'REVOKE ALL ON pg_auth_members FROM public'
            'REVOKE ALL ON pg_database FROM public'
            'REVOKE ALL ON pg_tablespace FROM public'
            'REVOKE ALL ON pg_settings FROM public'
        ])

    def init_db(self):
        queries = [
            ('CREATE ROLE %s_users NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN', [self.super_user]),
            ('REVOKE ALL ON DATABASE %s FROM public', [self.db_name]),
        ]
        if not self.create_db:
            queries.insert(0, ('CREATE DATABASE %s WITH OWNER=%s', [self.db_name, self.super_user]))
        self.execute_many('template1', queries)
        self.execute_many(self.db_name, [
            ('CREATE SCHEMA %s', [self.super_user]),
            ('GRANT USAGE ON SCHEMA %s TO %s WITH GRANT OPTION', [self.super_user, self.super_user]),
            ('GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s', [self.super_user, self.super_user]),
            ('GRANT %s_users TO %s', [self.super_user, super_user]),
            ('ALTER DATABASE %s SET search_path="\$user",%s', [self.db_name, self.db_name]),
        ])

    def add_user(self, user):
        self.execute_many(self.db_name, [
            ('CREATE ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN', [user]),
            ('CREATE SCHEMA %s', [user]),
            ('GRANT USAGE ON SCHEMA %s TO %s WITH GRANT OPTION', [user, user]),
            ('GRANT USAGE ON SCHEMA %s TO %s WITH GRANT OPTION', [self.super_user, user]),
            ('GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s', [self.super_user, user]),
            ('GRANT CONNECT,TEMPORARY ON DATABASE %s TO %s', [self.db_name, user]),
            ('GRANT %s_users TO %s', [self.super_user, user]),
        ])

    def get_connection(self, user):
        db_conf = copy.deepcopy(connections.databases[self.db_name])
        db_conf['USER'] = user
        connections.databases[user] = db_conf
        return connections[user]
