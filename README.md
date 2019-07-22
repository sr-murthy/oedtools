[![PyPI version](https://badge.fury.io/py/oedtools.svg)](https://badge.fury.io/py/oedtools) [![Build Status](https://travis-ci.com/sr-murthy/oedtools.svg?branch=master)](https://travis-ci.com/sr-murthy/oedtools)

# oedtools

`oedtools` is a command-line OED file validation and query toolkit for the <a href="https://github.com/Simplitium/OED" target="_blank">Simplitium OED</a> (re)insurance exposure data format.

**Note**: the OED version that this repository and package are pinned to is 1.0.3.

The main features currently include

* validation of OED account (`acc`), location (`loc`), reinsurance info. (`reinsinfo`) and reinsurance scope (`reinsscope`) input CSV files
* searching for columns with required properties - headers (column names) containing certain substrings, column descriptions containing keywords, columns with certain data types, default values, required, nonnull etc.
* sampling any given column for randomly generated data consistent with the column range or data type range or a specific column validation function

Future features include

* searching for information about the OED data entities underlying the column data - location properties such as latitudes and longitudes, occupancy and construction codes, country codes, currency codes, peril codes, TIVs, types and codes for deductibles and limits, etc.

## Installation and Requirements

Installation is via `pip` (Python 3).

    pip install oedtools

The only package requirement is a Python >=3.6 interpreter. It is best to install and use the package in a Python virtual environment.

## Features

The command line interface is invoked via `oed` and provides two main command groups.

* `validate` (`oed validate`) - for validating files (column headers + data), or only the headers in files
* `columns` (`oed columns`) - for searching for columns with required properties, and for sampling column data

### Validation

#### Files (headers + data)

File validation is performed via `oed validate file`, and includes validation of the column headers and data.

    usage: oed validate file [-h] -f INPUT_FILE_PATH -t SCHEMA_TYPE
    
    optional arguments:
      -h, --help            show this help message and exit
      -f INPUT_FILE_PATH, --input-file-path INPUT_FILE_PATH
                            OED input file path
      -t SCHEMA_TYPE, --schema-type SCHEMA_TYPE
                            File schema type - "loc", "acc", "reinsinfo", or
                            "reinsscope"

Headers and data are validated separately, and a combined status report is printed to the console, e.g.

    (myvenv) $ oed validate file -t 'loc' -f /path/to/location.csv
    /path/to/location.csv:11:40: Invalid value "WWTC;WEC;BFR;OO1" in "LocPerilsCovered" - check the column or data type range: OED error: E371 Out of range data found in column

    /path/to/location.csv:18:493: Invalid value "-25000" in "LocMinDed6All" - check the column or data type range: OED error: E371 Out of range data found in column

    /path/to/location.csv:1:870: "SubArea" is not a valid column in any OED schema: OED error: E303 Not a valid column in any OED schema

    /path/to/location.csv:1:870: "SubArea" is an invalid column in the OED "loc" schema: OED error: E304 Not a valid column in the given OED schema

    /path/to/location.csv:2:928: Invalid data type for value "s" in "CondPriority" - expected type "<class 'int'>", found type "<class 'str'>": OED error: E351 Invalid data type(s) in column

    /path/to/location.csv:1:-1: "LocCurrency" is a required column in an OED "loc" file but is missing: OED error: E331 Missing required column in file

If there are no errors in the file this is indicated with a short message, e.g.

    (myvenv) $ oed validate file -t 'acc' -f /path/to/account.csv
    "/path/to/account.csv" file validation complete: no exceptions or errors

Header-related errors currently include

* headers not currently defined in any OED schema
* headers which are mandatory in the given file schema but not present in the file

Data-related errors currently include

* null values in non-null columns (a non-null column is defined as a column which must not contain any null values)
* column values with data types inconsistent with the data type defined for the column in the given schema, e.g. string values in an integer or floating point column
* values not in the defined range of a column (this can be either a specific column range defined in the values profile, or a range inferred from the column data type defined in the schema)

**Note**: data validation (and sampling) is facilitated via a pre-generated JSON <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/values.json" target="_blank">"values profile"</a> of the OED data entities and related "value groups" underlying the columns. This values profile defines (sub)categories of data, independently of the schemas, such as deductible and limit types and codes, latitudes and longitudes, occupancy codes and construction codes, peril codes, currency codes, country codes, etc., and associates groups of columns whose values fall in the same category. Currently the value groups defined in the values profile include

* attachments
* construction codes
* country codes
* coverage types
* currencies
* deductible codes
* deductible types
* deductibles
* geocoding
* limit codes
* limit types
* limits
* location properties
* occupancy types
* peril codes
* reins percentages
* reins risk levels
* reins types
* shares
* tivs
* years

#### Headers

This works in a very similar way to file validation, except that it is only for validating the headers in a given file. The headers can be provided either by providing a file path, or as a comma-separated string in quotation marks, e.g.

    (myvenv) $ oed validate headers -t 'loc' -f /path/to/location.csv
    /path/to/location.csv:1:870: "SubArea" is not a valid column in any OED schema: OED error: E303 Not a valid column in any OED schema

    /path/to/location.csv:1:870: "SubArea" is an invalid column in the OED "loc" schema: OED error: E304 Not a valid column in the given OED schema

    /path/to/location.csv:1:-1: "LocCurrency" is a required column in an OED "loc" file but is missing: OED error: E331 Missing required column in file

### Searching for Columns and Sampling Column Data

#### Searching for Columns

Columns can be queried using `oed columns info` - results are always printed to console as JSON, in ascending alphabetic order by (case insensitive) header.

    usage: oed columns info [-h] [-t SCHEMA_TYPES] [-m COLUMN_HEADERS]
                            [-d DESCRIPTIONS] [-r] [-n] [-e DEFAULTS]
                            [-p PYTHON_DTYPES] [-s SQL_DTYPES] [-y NUMPY_DTYPES]
                            [-a]

    optional arguments:
      -h, --help            show this help message and exit
      -t SCHEMA_TYPES, --schema-types SCHEMA_TYPES
                            List of file schema types; must be one of "acc",
                            "loc", "reinsinfo", "reinsscope" - a comma-separated
                            string enclosed in quotation marks
      -m COLUMN_HEADERS, --column-headers COLUMN_HEADERS
                            List of column headers or header substrings - a comma-
                            separated string enclosed in quotation marks
      -d DESCRIPTIONS, --descriptions DESCRIPTIONS
                            List of column descriptions or description substrings
                            - a comma-separated string enclosed in quotation marks
      -r, --required        Is the column a required column in the file?
      -n, --nonnull         Is the column required not to have any null values?
      -e DEFAULTS, --defaults DEFAULTS
                            List of default values - a comma-separated string
                            enclosed in quotation marks
      -p PYTHON_DTYPES, --python-dtypes PYTHON_DTYPES
                            List of Python data types - only "int", "float", "str"
                            are supported; a comma-separated string enclosed in
                            quotation marks
      -s SQL_DTYPES, --sql-dtypes SQL_DTYPES
                            List of SQL data types - a comma-separated string
                            enclosed in quotation marks
      -y NUMPY_DTYPES, --numpy-dtypes NUMPY_DTYPES
                            List of Numpy data types - a comma-separated string
                            enclosed in quotation marks
      -a, --headers-only    Only return the column headers

Here are four queries that illustrate the possibilities of `oed columns info`.

1. Display full column information for the `BuildingTIV` and `BITIV` columns only (header names are case insensitive in the query).

        (myvenv) $ oed columns info -m 'buildingtiv, bitiv'
        [
            {
                "blank": false,
                "column_range": [
                    0.0,
                    3.4e+38
                ],
                "column_sampling": "column range",
                "column_validation": "column range",
                "default": null,
                "desc": "Business Interruption (BI) Total Insured Value",
                "dtype_range": [
                    -3.4e+38,
                    3.4e+38
                ],
                "entity": "Loc",
                "field_name": "BITIV",
                "numpy_dtype": "float32",
                "oed_db_field_name": null,
                "oed_db_table": "Locations",
                "py_dtype": "float",
                "required": "R",
                "secmod": null,
                "sql_dtype": "real"
            },
            {
                "blank": false,
                "column_range": [
                    0.0,
                    3.4e+38
                ],
                "column_sampling": "column range",
                "column_validation": "column range",
                "default": null,
                "desc": "Building Total Insured Value",
                "dtype_range": [
                    -3.4e+38,
                    3.4e+38
                ],
                "entity": "Loc",
                "field_name": "BuildingTIV",
                "numpy_dtype": "float32",
                "oed_db_field_name": null,
                "oed_db_table": "Locations",
                "py_dtype": "float",
                "required": "R",
                "secmod": null,
                "sql_dtype": "real"
            }
        ]

    **Note**: the schema type (specified using option `-t`) isn't required if the columns you're looking for are unique.

2. Display the headers only of all columns in the `loc` file schema with the header substring `6all` and with the `int` or `float` (Python) data type.

        (myvenv) $ oed columns info -t 'loc' -m '6all' -p 'int, float' --headers-only
        [
            "LocDed6All (Loc)",
            "LocDedCode6All (Loc)",
            "LocDedType6All (Loc)",
            "LocLimit6All (Loc)",
            "LocLimitCode6All (Loc)",
            "LocLimitType6All (Loc)",
            "LocMaxDed6All (Loc)",
            "LocMinDed6All (Loc)"
        ]

    **Note 1**: as some OED column headers indicate coverage type at the tail end of the header (`1building`, `2other`, `3contents`, `4bi`, `5pd`, `6all`), the header substring option `-m` can be used, as above, to search for columns based on coverage type.

    **Note 2**: The schema type is displayed in parentheses for clarity, as some columns like `LocNumber` and `AccNumber` can be present in different file types (`LocNumber` can occur in a ``loc`` or ``reinsscope`` file, and `AccNumber` can occur in a `loc` or `acc` or `reinsscope` file).

3. Display the headers only of all required and non-null columns in the `acc` file schema.

        (myvenv) $ oed columns info -t 'acc' --required --nonnull --headers-only
        [
            "AccCurrency (Acc)",
            "AccNumber (Acc)",
            "PolNumber (Acc)",
            "PolPerilsCovered (Acc)",
            "PortNumber (Acc)"
        ]

4. Display the headers only of all columns in all the schemas whose descriptions contain the keyword "percent", i.e. we're looking here for all percentage-valued columns.

        (myvenv) $ oed columns info -d 'percent' --headers-only
        [
            "BrickVeneer (Loc)",
            "BuildingExteriorOpening (Loc)",
            "CededPercent (ReinsScope, ReinsInfo)",
            "DeemedPercentPlaced (ReinsInfo)",
            "LocParticipation (Loc)",
            "PercentComplete (Loc)",
            "PercentSprinklered (Loc)",
            "PlacedPercent (ReinsInfo)",
            "ScaleFactor (Acc)",
            "SurgeLeakage (Loc)",
            "TreatyShare (ReinsInfo)"
        ]

#### Sampling Column Data

Column data can be sampled using `oed columns sample`.

    (myvenv) $ oed columns sample --help
    usage: oed columns sample [-h] -t SCHEMA_TYPE -m COLUMN_HEADER
                              [-n SAMPLE_SIZE]

    optional arguments:
      -h, --help            show this help message and exit
      -t SCHEMA_TYPE, --schema-type SCHEMA_TYPE
                            List of file schema types; must be one of "acc",
                            "loc", "reinsinfo", "reinsscope" - a comma-separated
                            string enclosed in quotation marks
      -m COLUMN_HEADER, --column-header COLUMN_HEADER
                            Column header
      -n SAMPLE_SIZE, --sample-size SAMPLE_SIZE
                            Sample size

Here are three examples.

1. Sampling reins. peril code sequences 

        (myvenv) $ oed columns sample -t 'loc' -m 'locperil'
        [
            "BBF;QEQ;WSS;ZIC",
            "ORF;QEQ;QLS;QQ1",
            "AA1;BB1;QEQ;ZST",
            "BB1;MNT;QLS;ZIC",
            "MTR;QSL;WTC;ZZ1",
            "BSK;QSL;WTC;WW2",
            "BSK;QEQ;QSL;WW2",
            "MNT;QEQ;XX1;ZST",
            "BFR;OO1;WEC;XX1",
            "QQ1;WW1;XX1;ZIC"
        ]

    **Note 1**: sample size can be specified using the `-n` option, which has the default value of `10`.

    **Note 2**: Column sampling is based on the values profile that describes properties of OED data entities, not on columns defined in the schemas. This means that sampling a column whose values fall in the same subcategory in the values profile as that of another column will produce similar results, e.g. sampling `LocPeril` will produce similar results to sampling `AccPeril` or `ReinsPeril`, because all three fall into the category of `peril codes` in the values profile.

2. Sampling reins. info. currency codes.

        (myvenv) $ oed columns sample -t 'reinsinfo' -m 'reinscurrency'
        [
            "MOP",
            "SUR",
            "YER",
            "HKD",
            "ROL",
            "JOD",
            "RUR",
            "GHS",
            "MNT",
            "BYB"
        ]

3. Sampling loc. occupancy codes.

        (myvenv) $ oed columns sample -t 'loc' -m 'occupancycode'
        [
            3643,
            2696,
            3753,
            3743,
            1126,
            1382,
            2608,
            3951,
            2392,
            2163
        ]
