__all__ = [
    'get_columns'
]

from .schema import (
    get_schema,
)


def get_columns(
    schema_types=None,
    headers=None,
    descriptions=None,
    required=None,
    nonnull=None,
    defaults=None,
    python_dtypes=None,
    sql_dtypes=None,
    numpy_dtypes=None
):
    """
    Queries the master schema for columns by header, and various properties
    including whether the column is required, non-null, and by default values,
    Python data types, SQL data types and Numpy data types. Multiple property
    types are combined into an "AND" query, e.g. specifying the ``required``
    and ``nonull`` properties and header substring(s) will produce a list of
    column schemas for all columns whose headers contain the given header
    substrings and which are required by their respective file schemas.

    :param schema_types: List or tuple of schema types - chosen from `acc`,
                         `loc`, `reinsinfo`, `reinsscope`
    :type schema_types: list, tuple

    :param headers: List or tuple of column headers or (substrings of headers)
    :type headers: list, tuple

    :param descriptions: List or tuple of column descriptons or (substrings of
                         descriptions)
    :type descriptions: list, tuple

    :param required: Whether the column(s) are required (mandatory, R),
                     conditionally required (CR), or optional (O), in the
                     respective OED file(s) - a list of strings which are
                     substrings of the set {'R', 'CR', 'O'}
    :type required: str

    :param nonnull: Whether the column(s) are non-null (must not contain null
                    values) in the respective OED file(s)
    :type nonull: bool

    :param defaults: List or tuple of default values
    :type defaults: list, tuple

    :param python_dtypes: List or tuple of Python data types (or substrings of
                          data types) - chosen from ``int``, ``float``, ``str``
    type python_dtypes: list, tuple

    :param sql_dtypes: List or tuple of SQL dtype string (or substrings of
                       data types) - chosen from ``bigint``, ``bit``, ``char``,
                       ``date``, ``datetime``, ``decimal``, ``float``, ``int``,
                       ``nchar``, ``numeric``, ``nvarchar(max)``, ``real``,
                       ``smallint``, ``time``, ``timestamp``, ``tinyint``,
                       ``unsigned bigint``, ``unsigned bit``, ``unsigned int``,
                       ``unsigned smallint``, ``unsigned tinyint``,
                       ``varchar``, ``varchar(max)``, ``year``
    type sql_dtypes: list, tuple

    :param numpy_dtypes: List or tuple of Numpy data types (or substrings or data
                         types) - chosen from ``float32``, ``float64``,
                         ``int16``, ``int32``, ``int64``, ``int8``, ``object``,
                         ``uint16``, ``uint32``, ``uint64``, ``uint8``
    type numpy_dtypes: list, tuple

    :return: (Possibly empty) sorted list of dicts, one per matching column.
             Sorting is by header
    :rtype: list
    """
    master_schema = get_schema()
    results = list(master_schema.values())

    if not any([schema_types, headers, descriptions, required, nonnull, python_dtypes, sql_dtypes, numpy_dtypes]):
        return []

    if schema_types:
        results = [
            v for v in results
            for schema_type in schema_types
            if schema_type.lower() in v['entity'].lower()
        ]

    if headers:
        results = [
            v for v in results
            for header in headers
            if header.lower() in v['field_name'].lower()
        ]

    if descriptions:
        results = [
            v for v in results
            for desc in descriptions
            if desc.lower() in v['desc'].lower()
        ]

    if required is not None:
        results = [
            v for v in results
            if v['required'] in required
        ]

    if nonnull is not None:
        results = [
            v for v in results
            if not v['blank'] == nonnull
        ]

    if defaults:
        results = [
            v for v in results
            if v['default'] is not None and v['default'] in defaults
        ]

    if python_dtypes:
        results = [
            v for v in results
            if v['py_dtype'].lower() in python_dtypes
        ]

    if sql_dtypes:
        results = [
            v for v in results
            for sql_dtype in sql_dtypes
            if sql_dtype.lower() in v['sql_dtype'].lower()
        ]

    if numpy_dtypes:
        results = [
            v for v in results
            for np_dtype in numpy_dtypes
            if np_dtype.lower() in v['numpy_dtype'].lower()
        ]

    keys = sorted(set([(r['entity'].lower(), r['field_name'].lower()) for r in results]), key=lambda r: r[1])

    results = [master_schema[k] for k in keys]

    return results
