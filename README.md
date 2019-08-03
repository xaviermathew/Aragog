# Aragog
 Dask-based data ingester + explorer
 * Load any dataset quickly into sqllite (in parallel wherever possible)
 * Automatically infer schema
 * Generate django model classes to be able to explore the dataset via the ORM
 * Generate django admin list views based on configuration (with support for filters)


## Supported datasets
* Any dataset supported by dask
* [django-rest-framework](https://github.com/encode/django-rest-framework) API support
* Generic HTTP API support
* New ones may be added by subclassing `aragog.fetchers.base.Fetcher` which uses the python `iterator` protocol


## Known issues
* `id` is a reserved column name and if a dataset has a column by that name, it will have to be skipped (check `packages.yml.example` for example)
* All datasets are loaded on server start. This is done to be able to infer the schema and generate the model classes. This should instead be calculated once and persisted to the DB. Generating model classes can then by done by reading this persisted schema


## Instructions
1) Clone this repo
2) Rename `packages.yml.example` to `packages.yml` (or configure an appropriate location in `settings.PACKAGES_FILE`
3) `$ ./manage.py runserver`