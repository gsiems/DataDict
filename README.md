
## Introduction

**DataDict** is a relational database data meta-data extraction and
dictionary generating tool.

Currently supported databases are:

 * Postgresql
 * Oracle
 * Sqlite
 * MS Access (via MDBtools)

DataDict started out as a collection of meta-data queries used for
exploring and comparing Postgresql and Oracle databases. The basic tool
grew from there in order to generate consistent, comparable output that
could be run from Linux/Unix shell scripts and cron. Support for SQLite
was added in part as a sanity check for the basic design and MS Access
support was added for the benefit of $work.

## Usage

 `bin/data_dictionary.pl -c configuration_file`

## Configuration

See `example.config`.

The following parameters are currently supported when creating data
dicitonaries (Note that when schema and table names are used, they are
case sensitive and need to match the case of the database object
names):

#### database_dsn

Required. The perl DBD connection string used for connecting to the
database.

#### database_name

Required. The database name to use in generating the data dictionary.

#### database_user

The user account for connecting to the database. For databases that
require a login.

#### database_password

The password for connecting to the database. For databases that require
a login.

#### target_dir

Not required. The base directory to write the data dictionary to.
Defaults to the curent directory if not specified.

#### log_file

Not required. Specifies the file to log messages to.
Defaults to STDERR if no log file is specified.

#### log_level

Not required. Controls which message levels are written to the log.
Valid values are {OFF, FATAL, ERROR, WARNING, INFO, and DEBUG}.
Defaults to WARNING if no level is specified.

#### append_log_file

Not required. Indicates whether to append to the existing log file (if
any) or to overwrite it. Valid values are {1, 0}. The default (0) is to
over-write any pre-existing log life.

#### show_sql

Not required. {1, 0} Indicates whether or not to show the sql for
queries, views or materialized views. The default (0) is to not
extract/show queries.

#### schemas

Not required. The comma separated list of schemas to use in creating
the data dictionary. Defaults to all non-system schemas unless there is
an exclude_schemas parameter provided.

#### exclude_schemas

Not required. Global comma separated list of schemas to exclude from
the data dictionary.

#### exclude_tables

Not required. Global comma separated list of tables to exclude from the
data dictionary. These table names will be excluded regardless of the
schema or schemas that they are found in.

#### schema_name-include_tables

Not required. Schema specific comma separated list of tables to include
in the data dictionary. While there can be only be one
schema_name-include_tables parameter for any given schema, there can be
one for each schema in the dictionary.

#### schema_name-exclude_tables

Not required. Schema specific comma separated list of tables to exclude
from the data dictionary. While there can be only be one
schema_name-exclude_tables parameter for any given schema, there can be
one for each schema in the dictionary.

## License

Artistic License 2.0 (http://www.perlfoundation.org/artistic_license_2_0)

## Issues

 * Oracle materialized view dependencies. When a materialized view
 depends on one or more views then both the views and the dependencies
 for the views are included in the dependencies list for the
 materialized view. This is an issue with the data in the Oracle
 dba_dependencies view.
