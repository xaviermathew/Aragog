from collections import Counter
import copy
from datetime import date, datetime

from genson import SchemaBuilder, SchemaNode
from genson.schema.generators import GENERATORS
from genson.schema.generators.object import Object
from genson.schema.generators.scalar import Number, Boolean, Null, String
from genson.schema.generators.base import TypedSchemaGenerator
import numpy

from aragog.model_generation import normalize_genson_field_types
from aragog.utils import get_dicts_pandas, inject_base, merge_counters


def match_object(cls, obj):
    if obj in ['True', 'False']:
        return True
    else:
        return type(obj) in cls.PYTHON_TYPES


Number.PYTHON_TYPES += (numpy.int64, numpy.float64)
Boolean.PYTHON_TYPES = (Boolean.PYTHON_TYPE, numpy.bool_)
Boolean.match_object = classmethod(match_object)
EMPTY_VALUES = ('', None, [], ())


class FillRateMixin(object):
    num_null = 0

    def update_fill_rate(self):
        if True:
            self.num_null += 1

    def add_object(self, obj):
        super(FillRateMixin, self).add_object(obj)
        self.update_fill_rate()

    def to_schema(self):
        d = super(FillRateMixin, self).to_schema()
        d['num_null'] = self.num_null
        return d

    def add_schema(self, schema):
        self.num_null += schema['num_null']
        return super(FillRateMixin, self).add_schema(schema)


class StatsMixin(object):
    total = 0
    min_value = None
    max_value = None
    mean_value = None

    def update_stats(self, obj):
        new_total = self.total + 1
        if self.min_value is None:
            self.min_value = obj
        else:
            self.min_value = min(self.min_value, obj)

        if self.max_value is None:
            self.max_value = obj
        else:
            self.max_value = max(self.max_value, obj)

        if self.mean_value is None:
            self.mean_value = obj
        else:
            self.mean_value = ((self.mean_value * self.total) + obj) / new_total

        self.total = new_total


def stats_mixin_add_object(self, return_val, obj):
    self.update_stats(obj)
    return return_val


def stats_mixin_to_schema(self, return_val):
    d = return_val
    d['total'] = self.total
    d['min_value'] = self.min_value
    d['max_value'] = self.max_value
    d['mean_value'] = self.mean_value
    return d


def stats_mixin_add_schema(self, return_val, schema):
    new_total = self.total + schema['total']

    if self.min_value:
        self.min_value = max(self.min_value, schema['max_value'])
    else:
        self.min_value = schema['min_value']

    if self.max_value:
        self.max_value = max(self.max_value, schema['max_value'])
    else:
        self.max_value = schema['max_value']

    if self.mean_value:
        self.mean_value = ((self.mean_value * self.total) + (schema['mean_value'] * schema['total'])) / new_total
    else:
        self.mean_value = schema['mean_value']
    self.total = new_total
    return return_val


class ChoicesMixin(object):
    too_many_choices = False
    max_choices = 100

    def __init__(self, *args, **kwargs):
        super(ChoicesMixin, self).__init__(*args, **kwargs)
        self.choices = Counter()

    def add_choice(self, obj):
        # https://stackoverflow.com/questions/35826912/what-is-a-good-heuristic-to-detect-if-a-column-in-a-pandas-dataframe-is-categori
        if not self.too_many_choices:
            self.choices[obj] += 1
            if len(self.choices) > self.max_choices:
                self.choices = set()
                self.too_many_choices = True

    def add_object(self, obj):
        super(ChoicesMixin, self).add_object(obj)
        self.add_choice(obj)

    def to_schema(self):
        d = super(ChoicesMixin, self).to_schema()
        if not self.too_many_choices:
            d['choices'] = self.choices
        return d

    def add_schema(self, schema):
        if 'choices' in schema:
            self.choices = merge_counters(self.choices, schema['choices'])
        return super(ChoicesMixin, self).add_schema(schema)


class TotalMixin(object):
    total = 0


def total_mixin_to_schema(self, return_val):
    d = return_val
    d['total'] = self.total
    return d


def total_mixin_add_object(self, return_val, obj):
    self.total += 1
    return return_val


def total_mixin_add_schema(self, return_val, schema):
    self.total += schema['total']
    return return_val


def monkey_patch(klass, old_method, new_method):
    def wrapper(self, *args, **kwargs):
        results = old_method.im_func(self, *args, **kwargs)
        return new_method(self, results, *args, **kwargs)
    setattr(klass, old_method.im_func.__name__, wrapper)


def add_keyword(klass, keyword):
    curr = klass.KEYWORDS
    if not isinstance(curr, tuple):
        curr = (curr,)
    curr += (keyword,)
    klass.KEYWORDS = curr


inject_base(Object, TotalMixin)
monkey_patch(Object, Object.add_object, total_mixin_add_object)
monkey_patch(Object, Object.to_schema, total_mixin_to_schema)
monkey_patch(Object, Object.add_schema, total_mixin_add_schema)
# add_keyword(Object, 'total')

inject_base(SchemaNode, TotalMixin)
monkey_patch(SchemaNode, SchemaNode.add_object, total_mixin_add_object)
monkey_patch(SchemaNode, SchemaNode.to_schema, total_mixin_to_schema)
monkey_patch(SchemaNode, SchemaNode.add_schema, total_mixin_add_schema)

inject_base(Null, FillRateMixin)
# add_keyword(Null, 'num_null')

inject_base(String, ChoicesMixin)
inject_base(Boolean, ChoicesMixin)

inject_base(Number, ChoicesMixin, StatsMixin)
monkey_patch(Number, Number.add_object, stats_mixin_add_object)
monkey_patch(Number, Number.to_schema, stats_mixin_to_schema)
monkey_patch(Number, Number.add_schema, stats_mixin_add_schema)


class Date(TypedSchemaGenerator):
    """
    generator for string schemas - works for ascii and unicode strings
    """
    JS_TYPE = 'date'
    PYTHON_TYPE = date


class Datetime(TypedSchemaGenerator):
    """
    generator for string schemas - works for ascii and unicode strings
    """
    JS_TYPE = 'datetime'
    PYTHON_TYPE = datetime


def typecast(v):
    if v in EMPTY_VALUES:
        v = None
    if isinstance(v, basestring):
        try:
            v = int(v)
        except ValueError:
            try:
                v = float(v)
            except ValueError:
                pass
    return v


def typecast_datum(d):
    for k, v in d.items():
        d[k] = typecast(v)
    return d


def get_schema_for_data(data):
    builder = SchemaBuilder()
    for d in data:
        d = typecast_datum(d)
        builder.add_object(d)
    return builder.to_schema()


def normalize_schema(schema):
    schema = copy.deepcopy(schema)
    for field, field_config in schema['properties'].items():
        if field_config.get('choices'):
            field_config['choices'] = dict(field_config['choices'])
        if field_config.get('anyOf'):
            field_types = field_config.pop('anyOf')
            field_type = normalize_genson_field_types(field_types)
            field_config['type'] = field_type
            for _field_type in field_types:
                if _field_type.get('choices'):
                    field_config['choices'] = dict(_field_type['choices'])
    return schema


def merge_schemas(schemas):
    builder = SchemaBuilder()
    for schema in schemas:
        builder.add_schema(schema)
    return normalize_schema(builder.to_schema())


def get_schema_for_dataframe(df_chunk, *args, **kwargs):
    dicts = get_dicts_pandas(df_chunk)
    return get_schema_for_data(dicts)


GENERATORS += (Date, Datetime)
