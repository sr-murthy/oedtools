.. oedtools documentation master file, created by
   sphinx-quickstart on Sun Jul 21 00:28:19 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

oedtools Documentation
======================

``oedtools`` is a command-line file validation and query toolkit for the `Simplitium Open Exposure Data (OED) <https://github.com/Simplitium/OED>`_ (re)insurance exposure data format.

**Note**: the repository and package are based on the current OED
version 1.0.3 - this is stored in the `schema_version.txt <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/schema_version.txt>`_  file.

The main user-level features currently include

-  **OED file validation** - validation of OED account (``acc``),
   location (``loc``), reinsurance info. (``reinsinfo``) and reinsurance
   scope (``reinsscope``) input CSV files
-  **column queries** - searching for columns in the schema based on
   queryable properties such as headers (column names) or header
   substrings, column descriptions containing keywords, Python, SQL or
   Numpy data types, default values, required and/or nonnull properties
-  **sampling column data** - sampling any given column in the schemas
   for randomly generated data consistent with the column range or data
   type range or a specific column validation function

Validation and sampling are based on two types of interrelated but
independent data structures built in to the package.

-  **file schemas** - separate JSON files for the `acc <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/acc_schema.json>`_ ,
   `loc <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/loc_schema.json>`_ , \ `reinsinfo <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/reinsinfo_schema.json>`_  and `reinsscope <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/reinsscope_schema.json>`_ files defining
   the properties of each column in each file
-  a **values profile** - a `JSON profile of data categories (and subcategories) <https://github.com/sr-murthy/oedtools/blob/master/oedtools/schema/values.json>`_ that can occur in the various columns, including
   categories and subcategories of values, column headers and specific
   column ranges associated with the subcategories (if they exist), and
   column data validation and sampling methods (where available).

The schemas define the column structure of OED files and provide a "column view" of the files, and the values
profile defines the properties of the data that occur in the columns and provides a "data" view of the files.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation-and-requirements
   features
   modules

Indices and tables
==================

* :ref:``genindex``
* :ref:``modindex``
* :ref:``search``
