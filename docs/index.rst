.. oedtools documentation master file, created by
   sphinx-quickstart on Sun Jul 21 00:28:19 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

oedtools Documentation
======================

``oedtools`` is a command-line file validation and query toolkit for the `Simplitium Open Exposure Data (OED) <https://github.com/Simplitium/OED>`_ (re)insurance exposure data format.

**Note**: the OED version that this repository and package are pinned to is 1.0.3.

The main features currently include

* validation of OED account (``acc``), location (``loc``), reinsurance info. (``reinsinfo``) and reinsurance scope (``reinsscope``) input CSV files
* searching for columns with required properties - headers (column names) containing certain substrings, column descriptions containing keywords, columns with certain data types, default values, required, nonnull etc.
* sampling any given column for randomly generated data consistent with the column range or data type range or a specific column validation function

Future features include

* searching for information about the OED data entities underlying the column data - location properties such as latitudes and longitudes, occupancy and construction codes, country codes, currency codes, peril codes, types and codes for deductibles and limits, etc.

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
