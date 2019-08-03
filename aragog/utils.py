import csv
from collections import Counter
from io import StringIO


def get_dicts(df):
    header = list(df._meta.columns)
    records=df.to_records().compute()
    dicts = [dict(zip(header, tuple(r)[1:])) for r in records]
    return dicts


def get_dicts_pandas(df):
    index_name = df.index.name
    for idx, row in df.iterrows():
        d = row.to_dict()
        if index_name:
            d[index_name] = idx
        yield d


def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def write_to_db(df_chunk, table_name, **kwargs):
    df_chunk.to_sql(name=table_name, if_exists='replace', index_label='id', **kwargs)


def ordered_uniques(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def inject_base(cls, *base_classes):
    try:
        cls.__bases__ = tuple(ordered_uniques(base_classes + cls.__bases__))
    except TypeError as ex:
        if cls.__bases__ == (object,) and any([1 for bc in base_classes if bc.__bases__ == (object,)]):
            for bc in reversed(base_classes):
                for attr in dir(bc):
                    if not attr.startswith('__'):
                        setattr(cls, attr, getattr(bc, attr))
        else:
            raise ex


def merge_counters(c1, c2):
    c = Counter(c1)
    for k, freq in c2.items():
        c[k] += freq
    return c
