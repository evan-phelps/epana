# epana

The **`epana`** package provides some general functions for exploratory data analysis and descriptive statistics.  It is not meant to provide a complete set of tools but rather to capture some functions/algorithms/processes that I've found myself repeating even while using high-level libraries that are openly available.  The project, additionally, contains scripts and notebooks related to very specific data handling and analysis.

Currently, the packaged utility functions cover the following general areas:

1. Logging -- `logutils.py`
1. Reading Encrypted Files -- `cryptic.py`
1. Characters and Encoding -- `scrubdub.py`
1. Tabular Data -- `tabular.py`
1. Relational Data -- `crosstabular.py`

***All of these modules need to be refactored to operate on file-like objects (i.e., like the file object returned by `open`; e.g., StringIO or BytesIO) instead of on file names.***  This will provide for more graceful piping or process chaining.  Alternative, and more permissively, they could operate on any object with a `read()` method (as in the approach of `pandas.read_csv`).

The above modules make direct, heavy use of the following third-party packages, which are build requirements:

1. `pandas`
1. `numpy`
1. `cchardet`
1. `ftfy`

The `cryptic` module, additionally, uses `paramiko` and `python-gnupg` but they are not listed as build requirements.

## Logging

### `logutils.py`

Provides a wrapper for Python's `logging` module.

1. Defines five log levels: `debug`, `info`, `warning`, `error`, and `critical`.
1. The function `get_logger` configures basic logging and takes only a `logname`, which will often be of function-level specificity.
1. The function `mstime` returns the current time in milliseconds.

TODO: Create a log decorator that automatically sets the log name based on module and function name, gets the logger, and logs entry/exit debug (or info) messages with timings.

## Reading Encrypted Files

### `cryptic.py`

***This module is not very portable.  It assumes Linux, GPG2, and `known_hosts` locations.***

Provides functions for reading PGP-encrypted files and, also, insults principles of coherence by providing functions to get remote files via SSH.

1. The function `head` decrypts the first compression block of a file, remote or local, and displays `N` lines or bytes.
1. The function `fopen` is a context for opening files and read in bytes mode but can decrypt inline and can take a local file handle or a remote url in the form `user@server:path`.

## Characters and Encoding

### `scrubdub.py`

Provides functions that operate on byte or character arrays, streams, or buffers for the purpose of character, string, and data type classification.

1. Guess and fix file encoding with functions `guess_encoding` and `fix_unicode_and_copy`.
1. Count characters and extract patterns with functions `count_chars`, `count_charclasses`, `tag_chrs`, and `chunk_chrs`.
1. And other stuff.

TODO: Add smart data-type and semantic classification (like determining if a string is a valid identifier in different coding schemes) based on character patterns.

## Tabular Data

### `tabular.py`

Loads, reduces, and summarizes tabular data.  Currently does not support fixed-width-field data.

1. Guess the field-separated dialect with `guess_dialect`.
1. Load tabular data into Pandas `DataFrame`s using `get_df_raw`, `load_files`, or `df_from_sql`.
1. Operate on Pandas `Series` with various aggregating functions.
1. Look into in-memory footprint of data and reduce the size of dataframes using `get_mem_usage`, `get_reduced_dtypes`, and `shrink_df`.
1. Summarize the content of dataframes using `freq` and `get_summary`.
1. and more.

## Relational Data

### `crosstabular.py`

Counts relational patterns between different tables of data.  This is valuable when receiving relational data in ways that do not enforce relational integrity.

## Dimensional Reduction

TODO: Add modules for dimensional reduction, especially in relational data under assumptions of there being central entities specified.

## Statistical Functions and Tests

TODO: Add stats modules.
