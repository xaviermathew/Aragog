import yaml

import dask
import dask.dataframe as dd
from django.conf import settings
from django.contrib import admin
from django.core.exceptions import ImproperlyConfigured
from sqlalchemy import create_engine

from aragog.fetchers.api import DRFFetcher, GenericAPIFetcher
from aragog.type_detection import get_schema_for_dataframe, merge_schemas
from aragog.model_generation import get_model_from_schema, get_sqla_schema_from_schema
from aragog.utils import write_to_db

# @note: multiprocess doesn't work at the moment for some reason
dask.config.set(scheduler='single-threaded')


class FakeArray(list):
    def __init__(self, *args, **kwargs):
        super(FakeArray, self).__init__(*args, **kwargs)
        self.ndim = 2
        self.dtype = None
        data = self
        d = data[0]
        self.shape = [len(data), len(d)]
        self.columns = d.keys()


class Dataset(object):
    def __init__(self, type, params, name=None, admin=None):
        self.type = type
        self.name = name
        self.params = params
        self.admin = admin

    def trim_df(self, df):
        if 'columns' in self.params:
            to_keep = set(self.params['columns'])
            to_drop = [c for c in df.columns if c not in to_keep]
            df = df.drop(to_drop, axis=1)
        return df

    def get_data_frame(self):
        type = self.type
        if hasattr(dd, 'read_%s' % type):
            reader = getattr(dd, 'read_%s' % type)
            df = reader(self.params['path'])
            df = self.trim_df(df)
        elif type == 'api_drf':
            fetcher_params = {'url': self.params['url']}
            if 'credentials' in self.params:
                fetcher_params['credentials'] = tuple(self.params['credentials'])
            data = FakeArray(DRFFetcher(**fetcher_params))
            df = dd.from_array(data, columns=data.columns)
            df = self.trim_df(df)
        elif type == 'api_generic':
            fetcher_params = {'url': self.params['url']}
            if 'credentials' in self.params:
                fetcher_params['credentials'] = tuple(self.params['credentials'])
            data = FakeArray(GenericAPIFetcher(**fetcher_params))
            df = dd.from_array(data, columns=data.columns)
            df = self.trim_df(df)
        elif type == 'multi':
            df_set = []
            for d in self.params['sources']:
                df = Dataset(**d).get_data_frame()
                df = df.set_index(self.params['join_params']['on'])
                df = self.trim_df(df)
                df_set.append(df)
            df = df_set[0]
            for _df in df_set[1:]:
                df = df.merge(_df, left_index=True, right_index=True, how=self.params['join_params']['how'])
            df = df.reset_index()
        else:
            raise ImproperlyConfigured('unknown dataset type:%s' % type)
        return df


def init_model(ds):
    table_name = ds.name.lower()
    print 'initing model name:%s type:%s params:%s' % (ds.name, ds.type, ds.params)
    # print 'sample:\n', df.head()
    df = ds.get_data_frame()
    schemas = df.map_partitions(get_schema_for_dataframe)
    schemas = schemas.compute()
    print 'schemas generated for %s partitions' % df.npartitions
    schema = merge_schemas(schemas)
    model_class = get_model_from_schema(ds.name, schema, {'app_label': 'aragog', 'db_table': table_name})
    print 'model generated'
    globals()[ds.name] = model_class
    engine = create_engine('sqlite:///sqlalchemy_example.db')
    # engine = create_engine('postgresql://scott:tiger@localhost/mydatabase')
    sqla_schema = get_sqla_schema_from_schema(schema)
    df.map_partitions(write_to_db, table_name=table_name, con=engine, dtype=sqla_schema).compute()
    return model_class


def register_admin(model_class, config):
    print 'initing admin name:%s' % model_class
    attrs = {}
    admin_config = config['admin']
    if 'list_display' in admin_config:
        attrs['list_display'] = admin_config['list_display']
    if 'list_filter' in admin_config:
        attrs['list_filter'] = admin_config['list_filter']
    admin_class = type(str('%sAdmin' % config['name']), (admin.ModelAdmin,), attrs)
    admin.site.register(model_class, admin_class)


with open(settings.PACKAGES_FILE) as package_file:
    for config in yaml.load(package_file):
        ds = Dataset(**config)
        model_class = init_model(ds)
        if 'admin' in config:
            register_admin(model_class, config)
