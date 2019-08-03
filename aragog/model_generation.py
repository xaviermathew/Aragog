from django.db import models
from sqlalchemy import types
# from django.contrib.postgres.fields import ArrayField, JSONField


DJANGO_FIELD_TYPE_MAP = {
    'string': models.TextField,
    'integer': models.BigIntegerField,
    'number': models.FloatField,
    'boolean': models.BooleanField,
    # 'object': JSONField,
    # 'array': ArrayField,
    'date': models.DateField,
    'datetime': models.DateTimeField,

    'default': models.TextField,
    'array': models.TextField,
}
SQLA_FIELD_TYPE_MAP = {
    'string': types.Text,
    'integer': types.BigInteger,
    'number': types.Float,
    'boolean': types.Boolean,
    # 'object': JSONField,
    # 'array': ArrayField,
    'date': types.Date,
    'datetime': types.DateTime,

    'default': types.String,
    'array': types.String,
}


def normalize_genson_field_types(field_types):
    if not isinstance(field_types, list):
        field_types = [field_types]

    if len(field_types) == 0:
        field_type = 'string'
    elif len(field_types) == 1:
        field_type = field_types[0]
    elif len(field_types) > 1:
        if 'string' in field_types or any([1 for d in field_types if d['type'] == 'string']):
            field_type = 'string'
        elif len(field_types) == 2 and any([1 for d in field_types if d['type'] == 'null']):
            field_type = [d for d in field_types if d['type'] != 'null'][0]['type']
        else:
            raise ValueError('dont know how to resolve field type among %s' % field_types)
    if field_type == 'null':
        field_type = 'string'
    return field_type


def get_field_config_for_genson_field(field_name, field_types, required):
    field_type = normalize_genson_field_types(field_types)
    field_class = DJANGO_FIELD_TYPE_MAP[field_type]
    args = ()
    is_null = field_name not in required
    kwargs = {'null': is_null, 'blank': is_null}
    return field_class, args, kwargs


def get_model_config_from_schema(schema):
    model_config = {}
    for field_name, field_config in schema['properties'].items():
        field_class, args, kwargs = get_field_config_for_genson_field(field_name,
                                                                      field_config.get('type', field_config.get('anyOf')),
                                                                      schema['required'])
        model_config[field_name] = {
            'field_class': field_class,
            'args': args,
            'kwargs': kwargs
        }
    return model_config


def get_model_from_model_config(name, model_config, meta_config, schema):
    name = str(name)
    bases = (models.Model,)
    Meta = type(str('Meta'), (object,), meta_config)
    attrs = {
        'Meta': Meta,
        '__module__': 'aragog.models',
        'source_config': {
            'model_config': model_config,
            'meta_config': meta_config,
            'schema': schema,
        }
    }
    for field_name, field_config in model_config.items():
        attrs[field_name] = field_config['field_class'](*field_config['args'], **field_config['kwargs'])

    model_class = type(name, bases, attrs)
    globals()['name'] = model_class
    return model_class


def get_model_from_schema(name, schema, meta_config):
    model_config = get_model_config_from_schema(schema)
    return get_model_from_model_config(name, model_config, meta_config, schema)


def get_sqla_field_config_for_genson_field(field_types):
    field_type = normalize_genson_field_types(field_types)
    field_class = SQLA_FIELD_TYPE_MAP[field_type]
    return field_class


def get_sqla_schema_from_schema(schema):
    model_config = {}
    for field_name, field_config in schema['properties'].items():
        field_class = get_sqla_field_config_for_genson_field(field_config.get('type', field_config.get('anyOf')))
        model_config[field_name] = field_class
    return model_config
