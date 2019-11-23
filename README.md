[![LGTM Code Quality Grade: Python](https://img.shields.io/lgtm/grade/python/g/sr-murthy/oedtools.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/sr-murthy/oedtools/context:python)
[![PyPI version](https://badge.fury.io/py/oedtools.svg)](https://badge.fury.io/py/oedtools)
[![Build Status](https://travis-ci.com/sr-murthy/oedtools.svg?branch=master)](https://travis-ci.com/sr-murthy/oedtools)

# oedtools

`oedtools` is a command-line file validation, query and data sampling toolkit for the <a href="https://github.com/Simplitium/OED" target="_blank">Simplitium Open Exposure Data (OED)</a> (re)insurance exposure data format.

**Note**: the repository and package are based on the current OED version 1.0.4 - this is stored in the <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/schema_version.txt" target="_blank">schema version</a> file.

The main user-level features currently include

* **validating files** - command-line validation (headers + data) of OED account (`acc`), location (`loc`), reinsurance info. (`reinsinfo`) and reinsurance scope (`reinsscope`) input CSV files
* **querying schemas** - command-line querying of columns in the various schemas based on properties such as headers (column names) or header substrings, column descriptions containing keywords, Python, SQL or Numpy data types, default values, and required and/or nonnull properties
* **sampling columns** - command-line sampling of column data, consistent with the column range or data type range or a specific column validation function

(The query toolkit will be augmented in future releases with the ability to query the values profile, which currently can only be examined directly as a dict.)

Validation, querying and sampling are all based on two types of interrelated but independent data structures built in to the package.

* **file schemas** - separate JSON files for the <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/acc_schema.json" target="_blank"> acc.</a>, <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/loc_schema.json" target="_blank">loc.</a>, <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/reinsinfo_schema.json" target="_blank">reins. info.</a> and <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/reinsscope_schema.json" target="_blank">reins. scope</a> files defining the properties of each column in each file
* a **values profile** - a <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/values.json" target="_blank">JSON profile</a> of the data that the files can store, but independent of considerations of the column structure, including categories and subcategories of values, column headers and specific column ranges associated with the subcategories (if they exist), and column data validation and sampling methods (where available).

The schemas define the column structure of OED files and provide a "column view" of the files, and the values
profile defines the properties of the data that occur in the columns and provides a "data view" of the files.

## Installation and Requirements

Installation is via `pip` (Python 3).

    pip install oedtools

The package requires a Python >=3.6 interpreter. It is best to install and use the package in a Python virtual environment.

## Features

The command line interface is invoked via `oed` and provides three main command groups.

* `validate` (`oed validate`) - for validating files (column headers + data), or only the headers in files
* `query` (`oed query`) - for querying schema columns based on various schema properties
* `sample` (`oed sample`) - for sampling column data

There is a utility subcommand named `version` which can be used to get the OED schema version (currently `1.0.4`) the package uses, or the package version (currently `0.3.2`). Usage is

    $ oed version
    1.0.4

    $ oed version --package
    0.3.2

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

* **non-OED headers** - headers not currently defined in any OED schema
* **incompatible OED headers** - (OED) headers in a file incompatible with the file schema
* **required but missing** headers - headers which are mandatory in a given file schema but not present in an actual input file

Data-related errors currently include

* **null values in non-null columns** - a non-null column is defined as a column which must not contain any null values
* **column values with incompatible data types** - values with data types inconsistent with the column data type, as defined in the given schema, e.g. string values in an integer or floating point column
* **out of range values** - values not in the defined range of a column (this can be either a specific column range defined in the values profile, or a range inferred from the column data type defined in the schema)

**Note**: data validation (and sampling) is facilitated via the <a href="https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/values.json" target="_blank">values profile</a>, which defines the categories and subcategories of data values that can occur in the various columns, independently of the schemas. The values profile defines, where applicable, the ranges of values associated with each subcategory and links these ranges to columns in the relevant schemas. It also defines, where applicable, methods for validation and sampling. Currently, the categories of data covered by the values profile include

* **attachments**
* **construction codes**
* **country codes**
* **coverage types**
* **currencies**
* **deductible codes**
* **deductible types**
* **deductibles**
* **geocoding**
* **limit codes**
* **limit types**
* **limits**
* **location properties**
* **occupancy types**
* **peril codes**
* **reins. percentages**
* **reins. risk levels**
* **reins. types**
* **shares**
* **TIVs**
* **years**

This will be extended in future releases to cover all possible values.

#### Headers

This works in a very similar way to file validation, except that it is only for validating the headers in a given file. The headers can be provided either by providing a file path, or as a comma-separated string in quotation marks, e.g.

    (myvenv) $ oed validate headers -t 'loc' -f /path/to/location.csv
    /path/to/location.csv:1:870: "SubArea" is not a valid column in any OED schema: OED error: E303 Not a valid column in any OED schema

    /path/to/location.csv:1:870: "SubArea" is an invalid column in the OED "loc" schema: OED error: E304 Not a valid column in the given OED schema

    /path/to/location.csv:1:-1: "LocCurrency" is a required column in an OED "loc" file but is missing: OED error: E331 Missing required column in file

### Querying

Schema columns can be queried using `oed query` - results are always printed to console as JSON, in ascending alphabetic order by (case insensitive) header.

    usage: oed query [-h] [-t SCHEMA_TYPES] [-m COLUMN_HEADERS] [-d DESCRIPTIONS]
                     [-r REQUIRED] [-n] [-e DEFAULTS] [-p PYTHON_DTYPES]
                     [-s SQL_DTYPES] [-y NUMPY_DTYPES] [-a]

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
      -r REQUIRED, --required REQUIRED
                            Is the column required (R), conditionally required
                            (CR) or optional (O)?
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

Here are five queries that illustrate the possibilities of `oed query`.

1. Display full column information for the `BuildingTIV` and `BITIV` columns only (header names are case insensitive in the query).

        (myvenv) $ oed query -m 'buildingtiv, bitiv'
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

2. Display the headers only of all columns in the loc. file schema with the header substring `6all` and with the `int` or `float` (Python) data type.

        (myvenv) $ oed query -t 'loc' -m '6all' -p 'int, float' --headers-only
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

3. Display the headers only of all required and non-null columns in the acc. file schema.

        (myvenv) $ oed query -t 'acc' -r 'R' --nonnull --headers-only
        [
            "AccCurrency (Acc)",
            "AccNumber (Acc)",
            "PolNumber (Acc)",
            "PolPerilsCovered (Acc)",
            "PortNumber (Acc)"
        ]

4. Display the headers only of all required or conditionally required columns in the reins. info. file schema.

        (myvenv) $ oed query -t 'reinsinfo' -r 'R,CR' --headers-only
        [
            "InuringPriority (ReinsInfo)",
            "PlacedPercent (ReinsInfo)",
            "ReinsCurrency (ReinsInfo)",
            "ReinsNumber (ReinsInfo)",
            "ReinsPeril (ReinsInfo)",
            "ReinsType (ReinsInfo)"
        ]

5. Display the headers only of all columns in all the schemas whose descriptions contain the keyword "percent", i.e. we're looking here for all percentage-valued columns.

        (myvenv) $ oed query -d 'percent' --headers-only
        [
            "BrickVeneer (Loc)",
            "BuildingExteriorOpening (Loc)",
            "CededPercent (ReinsInfo, ReinsScope)",
            "DeemedPercentPlaced (ReinsInfo)",
            "LocParticipation (Loc)",
            "PercentComplete (Loc)",
            "PercentSprinklered (Loc)",
            "PlacedPercent (ReinsInfo)",
            "ScaleFactor (Acc)",
            "SurgeLeakage (Loc)",
            "TreatyShare (ReinsInfo)"
        ]

### Sampling

Columns can be sampled using `oed sample`.

    (myvenv) $ oed sample --help
    usage: oed sample [-h] -t SCHEMA_TYPE -m COLUMN_HEADER
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

        (myvenv) $ oed sample -t 'loc' -m 'locperil'
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

    **Note 2**: Column sampling is based on the values profile - this describes properties of OED data and is organized by groups and subgroups. This means that sampling a column whose values fall in the same group in the values profile as that of another column will produce similar results, e.g. sampling `LocPeril` will produce identical results to sampling `AccPeril` or `ReinsPeril`, because all grouped under `peril codes` in the values profile.

2. Sampling reins. info. currency codes.

        (myvenv) $ oed sample -t 'reinsinfo' -m 'reinscurrency'
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

        (myvenv) $ oed sample -t 'loc' -m 'occupancycode'
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

## Docker version

The package also also be used in an (Ubuntu) Docker container and a Docker file is available for building the image - to build the image run this command (from the base of the repository):

    $ docker build -f ./Dockerfile -t <image name> .

To run the image in a container and enter the container in a Bash shell use this command:

    $ docker run --name <container name> -itd <image name> && docker exec -it <container name> bash

The OED tools package will be available via the `oed` binary.

    root@b7a8467f92d4:/usr/local/data# oed
    usage: oed [-h] {query,sample,validate,version} ...

    Root command

    positional arguments:
      {query,sample,validate,version}
        query               query
        sample              sample
        validate            validate
        version             version

    optional arguments:
      -h, --help            show this help message and exit

## Contributors

Developer contributions are welcome, in the usual way - fork the repository; create a feature and/or fix branch off `master`; make, test and commit your changes to the branch; create a PR from the base branch against this repository. Linting the code with PEP8 and/or Flake8 would be appreciated (ignoring E501). The test runner is `pytest`. Run all the tests (from the repo. root) with

    $ pytest -v tests

To run a specific test module use

    $ pytest -v tests/<test module name>.py

To run a run specific test class in a test module use

    $ pytest -v tests/<test module name>.py::<test class name>>

To run a run specific test case in a test class in a test module use

    $ pytest -v tests/<test module name>.py::<test class name>>::<test case name>>
