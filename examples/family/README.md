Family migration example
========================

Synopsis
--------
This directory contains the Jinx migration Family example.

The Family example demonstrates how to load the content of a source CSV file into
a Family data store. The use cases illustrate several common migration impediments:

* Different source-destination terminology
* Different source-destination associations
* Incomplete input
* Denormalized input
* Inconsistent input
* Input data scrubbing

Migration
---------
The example migration input data resides in the `data` directory.
Each `parents` CSV input file holds one row for each parent.
Each `childs` CSV input file holds one row for each parent.

Each input file has a corresponding migration mapping configuration in the `conf` directory.
