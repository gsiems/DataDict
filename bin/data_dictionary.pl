#!/usr/bin/env perl
use strict;
use warnings;
use File::Spec;
use Template;
use Data::Dumper;

BEGIN {
    use File::Spec;
    my $script_dir = ( File::Spec->splitpath( File::Spec->rel2abs($0) ) )[1];
    $script_dir =~ s|bin[/\\]$||;
    $script_dir = File::Spec->catdir( $script_dir, 'lib' );
    unshift @INC, $script_dir;
}

use DataDict::Extractor;
use DataDict::Logger;
use DataDict::Writer;
use DataDict::Config;
use DataDict::Util;
use DataDict::Graph::Dependency;

my $config    = DataDict::Config->read_config();
my $logger    = $config->get_value('logger') || DataDict::Logger->new_logger();
my $extractor = DataDict::Extractor->new_extractor( 'config' => $config );
my $util      = DataDict::Util->new_util( 'config' => $config );

my $wiki_format = $config->get_value('wiki_format');
my $formatter;
if ( $wiki_format && $wiki_format eq 'Markdown' ) {
    use Text::Markdown 'markdown';
    $formatter = Text::Markdown->new();
}

my %tt_vars;
foreach (qw(database_name bin_file run_timestamp no_js)) {
    if ( exists $config->{$_} ) {
        $tt_vars{$_} = $config->{$_};
    }
}
my $db_version = $extractor->get_db_version() || '';
my $db_comment = format_comment( $extractor->get_db_comment() || '' );
my $db_encoding = $extractor->get_db_encoding() || '';

$logger->log_debug("Db Version is $db_version ...");
$logger->log_debug("Db Encoding is $db_encoding ...");
$logger->log_debug("Db Comment is $db_comment ...");

$tt_vars{database_version}  = $db_version;
$tt_vars{database_comment}  = $db_comment;
$tt_vars{database_encoding} = $db_encoding;

my $fs = $extractor->get_objects('FOREIGN DATA SERVER');
my @foreign_servers;
if ($fs) {
    foreach my $srv ( sort keys %{$fs} ) {
        my $srv_owner   = $fs->{$srv}{srv_owner};
        my $fdw_name    = $fs->{$srv}{fdw_name};
        my $srv_options = $fs->{$srv}{srv_options};
        my $srv_comment = format_comment( $fs->{$srv}{comments} );
        push @foreign_servers, [ $srv, $srv_owner, $fdw_name, $srv_options, $srv_comment ];
    }
}
$tt_vars{has_foreign_servers} = ( @foreign_servers && scalar @foreign_servers ) ? 1 : 0;

my %schemas = get_schemas();
if ( %schemas && keys %schemas ) {
    my %indices;

    foreach my $schema_name ( sort keys %schemas ) {
        $logger->log_info("Extracting index list for $schema_name ...");
        $extractor->set_schema($schema_name);
        my $idx = $extractor->get_objects('INDEX');

        foreach my $table_name ( keys %{$idx} ) {
            foreach my $index_name ( keys %{ $idx->{$table_name} } ) {
                next unless ( exists $idx->{$table_name}{$index_name}{column_names} );
                my $column_names = join( ',', @{ $idx->{$table_name}{$index_name}{column_names} } );
                $indices{$schema_name}{$table_name}{$column_names}++;
            }
        }
    }

    my @schema_list;
    foreach my $schema_name ( sort keys %schemas ) {
        $logger->log_info("Building data dictionary for $schema_name ...");
        $extractor->set_schema($schema_name);

        my $schema_owner = $schemas{$schema_name}{schema_owner};
        my $comments     = format_comment( $schemas{$schema_name}{comments} );
        push @schema_list, [ $schema_name, $schema_owner, $comments ];

        $tt_vars{schema_name}    = $schema_name;
        $tt_vars{schema_comment} = $comments;

        my %objects;

        # Oracle materialized views don't play well if they are in need of compilation
        $extractor->compile_schema();

        $objects{'DOMAIN'} = $extractor->get_objects('DOMAIN');

        my $table_filter = $config->get_table_filter($schema_name);

        $objects{$_} = $extractor->get_objects( $_, $table_filter )
            for (
            'TABLE',            'PRIMARY KEY',
            'COLUMN',           'INDEX',
            'CHECK CONSTRAINT', 'DEPENDENCY',
            'DEPENDENT',        'FOREIGN DATA WRAPPER',
            'FOREIGN KEY',      'CHILD KEY',
            'UNIQUE CONSTRAINT',
            );

        # TYPE support would be nice, but they appear to be implemented
        # so differently between different databases...

        #        my @table_order = $extractor->get_table_dependency_order($table_filter);
        #        if (@table_order) {
        #            print join ("\n", map { "$schema_name.$_" } @table_order) . "\n\n";
        #        }
        #        print Dumper \%objects;

        my @col_oddities = build_column_oddities_list( $schema_name, \%objects );
        my @tab_oddities = build_table_oddities_list( $schema_name, \%objects, \%indices );
        my @domains = build_domain_list( $schema_name, \%objects );
        my @columns = build_column_list( $schema_name, \%objects );
        my @tables = build_table_list( $schema_name, \%objects );
        my @uniques = build_uniq_constraint_list( $schema_name, \%objects );
        my @fkeys = build_fk_constraint_list( $schema_name, \%objects, \%indices );
        my @checks = build_chk_constraint_list( $schema_name, \%objects );

        $tt_vars{has_constraints} =
            ( ( @uniques && scalar @uniques ) || ( @checks && scalar @checks ) || ( @fkeys && scalar @fkeys ) ) ? 1 : 0;

        $tt_vars{has_domains} = ( @domains && scalar @domains ) ? 1 : 0;
        $tt_vars{has_oddness} =
            ( ( @col_oddities && scalar @col_oddities ) || ( @tab_oddities && scalar @tab_oddities ) ) ? 1 : 0;

        $tt_vars{'columns'} = \@columns if ( @columns && scalar @columns );
        process_template( "columns.tt", \%tt_vars, "$schema_name/columns.html" );
        delete $tt_vars{'columns'};

        $tt_vars{'domains'} = \@domains if ( @domains && scalar @domains );
        process_template( "domains.tt", \%tt_vars, "$schema_name/domains.html" );
        delete $tt_vars{'domains'};

        $tt_vars{'check_constraints'}  = \@checks  if ( @checks  && scalar @checks );
        $tt_vars{'unique_constraints'} = \@uniques if ( @uniques && scalar @uniques );
        $tt_vars{'fk_constraints'}     = \@fkeys   if ( @fkeys   && scalar @fkeys );
        process_template( "constraints.tt", \%tt_vars, "$schema_name/constraints.html" );
        delete $tt_vars{'check_constraints'};
        delete $tt_vars{'unique_constraints'};
        delete $tt_vars{'fk_constraints'};

        $tt_vars{'tables'} = \@tables if ( @tables && scalar @tables );
        process_template( "tables.tt", \%tt_vars, "$schema_name/tables.html" );
        delete $tt_vars{'tables'};

        $tt_vars{'column_oddities'} = \@col_oddities if ( @col_oddities && scalar @col_oddities );
        $tt_vars{'table_oddities'}  = \@tab_oddities if ( @tab_oddities && scalar @tab_oddities );

        process_template( "odd-things.tt", \%tt_vars, "$schema_name/odd-things.html" );
        delete $tt_vars{'column_oddities'};
        delete $tt_vars{'table_oddities'};

        $logger->log_info("Building dependency graph for $schema_name ...");
        my $dep_graph = DataDict::Graph::Dependency->new_graph(
            'format'         => 'gml',
            'schema'         => $schema_name,
            'objects'        => \%objects,
            'schema_comment' => $comments,
            'db_version'     => $db_version,
            'db_comment'     => $db_comment,
            'database_name'  => $config->{database_name} || '',
        );

        write_file( "$schema_name/dependencies.gml", $dep_graph->graph() );

        # TODO: add dependencies in dot format
        # TODO: add ERD in gml format
        # TODO: add ERD in dot format

        process_template( "diagrams.tt", \%tt_vars, "$schema_name/diagrams.html" );

        build_table_doc( $schema_name, $db_version, \%objects, \%tt_vars, \%indices );
        delete $tt_vars{schema_name};
        delete $tt_vars{schema_comment};
    }

    if ( $tt_vars{has_foreign_servers} ) {
        $logger->log_debug("Writing foreign server list ...");
        $tt_vars{'foreign_servers'} = \@foreign_servers;
        process_template( "foreign_servers.tt", \%tt_vars, "foreign_servers.html" );

        delete $tt_vars{'foreign_servers'};
    }

    $logger->log_debug("Writing index ...");
    $tt_vars{'schemas'} = \@schema_list;
    process_template( "schemas.tt", \%tt_vars, "index.html" );

    delete $tt_vars{'schemas'};

    $util->init_html_destination( $config->{base_dir}, +( join '/', $config->{target_dir}, $config->{database_name} ) );
}

########################################################################

sub build_column_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building column list for $schema_name ...");
    my @data;

    foreach my $table_name ( sort keys %{ $objects->{'COLUMN'} } ) {
        $logger->log_debug("Parsing $table_name ...");
        my @column_names = @{ $objects->{'COLUMN'}{$table_name}{column_names} };
        my @data_types   = @{ $objects->{'COLUMN'}{$table_name}{data_types} };
        foreach my $idx ( 0 .. $#column_names ) {
            my $column_name = $column_names[$idx];
            my $data_type   = $data_types[$idx];
            my $position    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{ordinal_position};
            my $nullable    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{is_nullable};
            my $default     = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{data_default};
            my $comments    = format_comment( $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{comments} );
            push @data, [ $table_name, $column_name, $position, $data_type, $nullable, $default, $comments ];
        }
    }
    $logger->log_debug( scalar @data . " columns listed" );
    return @data;
}

sub build_chk_constraint_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building check constraint list for $schema_name ...");
    my @data;

    foreach my $table_name ( sort keys %{ $objects->{'CHECK CONSTRAINT'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'CHECK CONSTRAINT'}{$table_name} } ) {
            my $search_condition = $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{search_condition};
            my $status           = $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{status};
            my $comments         = format_comment( $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{comments} );
            unless ($search_condition) {
                $logger->log_warning("No search condition found for $table_name ($cons_name) check constraint");
                next;
            }
            next if ( $search_condition =~ m/^"[^"]+" IS NOT NULL/i );
            push @data, [ $table_name, $cons_name, $search_condition, $status, $comments ];
        }
    }
    $logger->log_debug( scalar @data . " check constraints listed" );
    return @data;
}

sub build_fk_constraint_list {
    my ( $schema_name, $objects, $indices ) = @_;
    $logger->log_info("Building foreign key constraint list for $schema_name ...");
    my @data;

    foreach my $table_name ( sort keys %{ $objects->{'FOREIGN KEY'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'FOREIGN KEY'}{$table_name} } ) {
            my $r_table_schema = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_schema};
            my $r_table_name   = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_name};
            my $comments       = format_comment( $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{comments} );
            my $cons_rule      = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{constraint_rule};
            my $status         = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{status};
            my $column_names   = join( ',', @{ $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{column_names} } );
            my $r_column_names = join( ',', @{ $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_column_names} } );
            my $has_index      = ( exists $indices->{$schema_name}{$table_name}{$column_names} ) ? 'Y' : 'N';

            push @data,
                [
                $table_name,   $cons_name,
                $column_names, $r_table_schema,
                $r_table_name, $r_column_names,
                $cons_rule,    $status,
                $comments,     $has_index
                ];
        }
    }
    $logger->log_debug( scalar @data . " foreign key constraints listed" );

    return @data;
}

sub build_uniq_constraint_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building unique constraint list for $schema_name ...");
    my @data;

    foreach my $table_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'}{$table_name} } ) {
            my $comments     = format_comment( $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{comments} );
            my $status       = $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{status};
            my $column_names = join( ',', @{ $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{column_names} } );
            push @data, [ $table_name, $cons_name, $column_names, $status, $comments ];
        }
    }
    $logger->log_debug( scalar @data . " unique constraints listed" );
    return @data;
}

sub build_type_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building type list for $schema_name ...");
    my @data;

    # TODO : type definitions appear to vary widely based on the database engine (say Oracle vs. PostgreSQL)

    foreach my $type_name ( sort keys %{ $objects->{'TYPE'} } ) {
        my $type_schema      = $objects->{'TYPE'}{$type_name}{object_schema};
        my $type_owner       = $objects->{'TYPE'}{$type_name}{object_owner};
        my $type_name        = $objects->{'TYPE'}{$type_name}{object_name};
        my $type_source      = $objects->{'TYPE'}{$type_name}{source} || '';
        my $type_body_source = $objects->{'TYPE'}{$type_name}{body_source} || '';

        # TODO : $type_source . $type_body_source

        push @data, [ $type_name, $type_owner, $type_name, $type_source ];
    }
    $logger->log_debug( scalar @data . " types listed" );
    return @data;
}

sub build_domain_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building domain list for $schema_name ...");
    my @data;

    foreach my $domain_name ( sort keys %{ $objects->{'DOMAIN'} } ) {
        my @ary =
            map { $objects->{'DOMAIN'}{$domain_name}{$_} }
            (qw(domain_owner data_type is_nullable domain_default check_constraint ));
        push @ary, format_comment( $objects->{'DOMAIN'}{$domain_name}{comments} );
        push @data, [ $domain_name, @ary ];
    }
    $logger->log_debug( scalar @data . " domains listed" );

    return @data;
}

sub build_table_oddities_list {
    my ( $schema_name, $objects, $indices ) = @_;
    $logger->log_info("Building table oddities list for $schema_name ...");
    my @data;

    my %pks  = ( exists $objects->{'PRIMARY KEY'} && $objects->{'PRIMARY KEY'} ) ? %{ $objects->{'PRIMARY KEY'} } : ();
    my %idxs = ( exists $objects->{'INDEX'}       && $objects->{'INDEX'} )       ? %{ $objects->{'INDEX'} }       : ();

    my $has_pks  = scalar keys %pks;
    my $has_idxs = scalar keys %idxs;

    # TODO : foreign key relationships where the child has no index

    # Tables that are not in a PK/FK relationship (hermits or orphans):
    # first list the tables that do have a relationship
    my %has_relationship;
    foreach my $table_name ( keys %{ $objects->{'FOREIGN KEY'} } ) {
        $has_relationship{$schema_name}{$table_name} = 1;
        foreach my $cons_name ( keys %{ $objects->{'FOREIGN KEY'}{$table_name} } ) {
            my $r_table_schema = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_schema};
            my $r_table_name   = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_name};
            $has_relationship{$r_table_schema}{$r_table_name} = 1;
        }
    }

    # Denormalized tables?
    # We are looking for columns that may have incrementing column names { col1, col2, etc. }
    my %denormalized;
    foreach my $table_name ( sort keys %{ $objects->{'COLUMN'} } ) {
        my %temp = ();
        foreach my $column_name ( @{ $objects->{'COLUMN'}{$table_name}{column_names} } ) {
            my $key = $column_name;
            $key =~ s/[0-9_]+$//;
            my ($val) = $column_name =~ m/([0-9]+)$/;
            $val ||= 0;
            push @{ $temp{$key} }, $val;
        }
        foreach my $key ( keys %temp ) {
            if ( scalar @{ $temp{$key} } > 1 ) {
                my @ary = sort { $a <=> $b } @{ $temp{$key} };
                foreach my $idx ( 1 .. $#ary ) {
                    if ( $ary[ $idx - 1 ] == $ary[$idx] - 1 ) {
                        $denormalized{$table_name} = 1;
                        last;
                    }
                }
            }
        }
    }

    foreach my $table_name ( sort keys %{ $objects->{'TABLE'} } ) {
        my %tab_oddities;
        my $table_type = $objects->{'TABLE'}{$table_name}{table_type};
        next unless ( $table_type eq 'TABLE' );
        my $row_count    = $objects->{'TABLE'}{$table_name}{row_count}    || '';
        my $column_count = $objects->{'TABLE'}{$table_name}{column_count} || '';

        my $no_pk    = ($has_pks)                                              ? ( !exists $pks{$table_name} )  : 0;
        my $no_index = ($has_idxs)                                             ? ( !exists $idxs{$table_name} ) : 0;
        my $orphan   = ( exists $has_relationship{$schema_name}{$table_name} ) ? 0                              : 1;
        my $denorm   = ( exists $denormalized{$table_name} )                   ? 1                              : 0;
        my $one_column  = ( '1' eq $column_count );
        my $empty_table = ( '0' eq $row_count );

        my $dup_index = 0;
        if ( exists $indices->{$schema_name}{$table_name} ) {
            foreach my $columns ( keys %{ $indices->{$schema_name}{$table_name} } ) {
                $dup_index = 1 if ( $indices->{$schema_name}{$table_name}{$columns} > 1 );
            }
        }

        next unless ( $no_pk || $no_index || $dup_index || $one_column || $empty_table || $orphan || $denorm );
        push @data, [ $table_name, $no_pk, $no_index, $dup_index, $one_column, $empty_table, $orphan, $denorm ];
    }
    $logger->log_debug( scalar @data . " tables with oddities listed" );
    return @data;
}

sub build_column_oddities_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building column oddities list for $schema_name ...");
    my @data;
    my %temp;

    # Unique constraints with nullable columns
    foreach my $table_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'}{$table_name} } ) {
            foreach my $column_name ( @{ $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{column_names} } ) {
                my $nullable = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{is_nullable} || '';
                if ( 'Y' eq $nullable ) {
                    $temp{$table_name}{$column_name}{null_constraint} = 1;
                }
            }
        }
    }

    # Unique indices with nullable columns
    foreach my $table_name ( sort keys %{ $objects->{'INDEX'} } ) {
        foreach my $idx_name ( sort keys %{ $objects->{'INDEX'}{$table_name} } ) {
            next unless ( 'Y' eq $objects->{'INDEX'}{$table_name}{$idx_name}{is_unique} );
            foreach my $column_name ( @{ $objects->{'INDEX'}{$table_name}{$idx_name}{column_names} } ) {
                my $nullable = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{is_nullable} || '';
                if ( 'Y' eq $nullable ) {
                    $temp{$table_name}{$column_name}{null_unique_index} = 1;
                }
            }
        }
    }

    # 'NULL' default values
    foreach my $table_name ( sort keys %{ $objects->{'COLUMN'} } ) {
        foreach my $column_name ( @{ $objects->{'COLUMN'}{$table_name}{column_names} } ) {
            my $default = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{data_default} || '';
            if ( $default =~ m/^['"]*null['"]*/i ) {
                $temp{$table_name}{$column_name}{hinky_null_default} = 1;
            }
        }
    }

    # TODO : Implied relationship to another tables PK (based on name/type) ?

    foreach my $table_name ( sort keys %temp ) {
        foreach my $column_name ( sort keys %{ $temp{$table_name} } ) {
            my $null_constraint    = $temp{$table_name}{$column_name}{null_constraint}    || 0;
            my $null_unique_index  = $temp{$table_name}{$column_name}{null_unique_index}  || 0;
            my $hinky_null_default = $temp{$table_name}{$column_name}{hinky_null_default} || 0;

            push @data, [ $table_name, $column_name, $null_constraint, $null_unique_index, $hinky_null_default ];
        }
    }

    $logger->log_debug( scalar @data . " columns with oddities listed" );
    return @data;
}

sub missing_indices {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info(
        "Building foreign keys where the child table has no corresponding index list for $schema_name ...");
    my @data;

    if ( exists $objects->{'FOREIGN KEY'} && exists $objects->{'INDEX'} ) {
        my %no_idx_fk;

        # foreign keys (parent)
        foreach my $table_name ( keys %{ $objects->{'FOREIGN KEY'} } ) {
            foreach my $cons_name ( keys %{ $objects->{'FOREIGN KEY'}{$table_name} } ) {
                my $r_table_schema = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_schema};
                my $r_table_name   = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_name};
                my $column_names   = join( ',', @{ $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{column_names} } );
                @{ $no_idx_fk{$table_name}{$column_names} } = ( $cons_name, $r_table_schema, $r_table_name );
            }
        }

        # remove those with indices
        foreach my $table_name ( keys %{ $objects->{'INDEX'} } ) {
            foreach my $index_name ( keys %{ $objects->{'INDEX'}{$table_name} } ) {
                next unless ( exists $objects->{'INDEX'}{$table_name}{$index_name}{column_names} );
                my $column_names = join( ',', @{ $objects->{'INDEX'}{$table_name}{$index_name}{column_names} } );
                if ( exists $no_idx_fk{$table_name}{$column_names} ) {
                    delete $no_idx_fk{$table_name}{$column_names};
                }
            }
        }

        foreach my $table_name ( sort keys %no_idx_fk ) {
            foreach my $column_names ( sort keys %{ $no_idx_fk{$table_name} } ) {
                my ( $cons_name, $r_table_schema, $r_table_name ) = @{ $no_idx_fk{$table_name}{$column_names} };
                push @data, [ $table_name, $column_names, $cons_name, $r_table_schema, $r_table_name ];
            }
        }
    }
    return @data;
}

sub build_table_list {
    my ( $schema_name, $objects ) = @_;
    $logger->log_info("Building table list for $schema_name ...");
    my @data;

    foreach my $table_name ( sort keys %{ $objects->{'TABLE'} } ) {
        my $table_owner  = $objects->{'TABLE'}{$table_name}{table_owner};
        my $table_schema = $objects->{'TABLE'}{$table_name}{table_schema};
        my $table_type   = $objects->{'TABLE'}{$table_name}{table_type};
        my $row_count    = $objects->{'TABLE'}{$table_name}{row_count};
        my $column_count = $objects->{'TABLE'}{$table_name}{column_count};
        my $comments     = format_comment( $objects->{'TABLE'}{$table_name}{comments} );

        $table_type =
              ( $table_type eq 'TABLE' )             ? 'T'
            : ( $table_type eq 'VIEW' )              ? 'V'
            : ( $table_type eq 'MATERIALIZED VIEW' ) ? 'MV'
            : ( $table_type eq 'FOREIGN TABLE' )     ? 'FT'
            :                                          '';

        push @data, [ $table_name, $table_owner, $table_type, $row_count, $column_count, $comments ];
    }

    $logger->log_debug( scalar @data . " tables listed" );
    return @data;
}

sub build_table_doc {
    my ( $schema_name, $db_version, $objects, $tt_vars, $indices ) = @_;
    $logger->log_info("Building table documentation for $schema_name ...");
    my %temp;

    # columns (and query for views/materialized views)
    foreach my $table_name ( sort keys %{ $objects->{'TABLE'} } ) {
        my $table_type = $objects->{'TABLE'}{$table_name}{table_type};
        $temp{$table_name}{row_count}   = $objects->{'TABLE'}{$table_name}{row_count}   || '';
        $temp{$table_name}{table_owner} = $objects->{'TABLE'}{$table_name}{table_owner} || '';
        $temp{$table_name}{table_comment} = format_comment( $objects->{'TABLE'}{$table_name}{comments} || '' );
        $temp{$table_name}{table_type} = $objects->{'TABLE'}{$table_name}{table_type};

        my @column_names = @{ $objects->{'COLUMN'}{$table_name}{column_names} };
        my @data_types   = @{ $objects->{'COLUMN'}{$table_name}{data_types} };
        foreach my $idx ( 0 .. $#column_names ) {
            my $column_name = $column_names[$idx];
            my $data_type   = $data_types[$idx];
            my $position    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{ordinal_position};
            my $nullable    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{is_nullable};
            my $default     = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{data_default};
            my $comments    = format_comment( $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{comments} );
            push @{ $temp{$table_name}{columns} },
                [ $column_name, $position, $data_type, $nullable, $default, $comments ];
        }

        my $query = $objects->{'TABLE'}{$table_name}{query} || '';
        if ( $query && $config->get_value('show_sql') ) {

            # Hack the SQL text to better display in the browser. Since
            # not all queries will be neatly formatted we want to allow
            # text wrapping (not using pre tag any more) while still
            # respecting wrapping/indentation of neatly formatted queries.
            $query =~ s|\n|\n<br/>|g;
            $query =~ s|^ +||;
            $query =~ s| +$||;
            $query =~ s|  |&nbsp; |g;

            $temp{$table_name}{query} = $query;

            if ( exists $objects->{'DEPENDENCY'} && exists $objects->{'DEPENDENCY'}{$table_name} ) {
                my @dependencies = @{ $objects->{'DEPENDENCY'}{$table_name} };
                foreach my $dependency (@dependencies) {
                    my $referenced_schema = $dependency->{referenced_schema};
                    my $referenced_name   = $dependency->{referenced_name};
                    my $referenced_type   = $dependency->{referenced_type};
                    push @{ $temp{$table_name}{dependencies} },
                        [ $referenced_schema, $referenced_name, $referenced_type ];
                }
            }
            elsif ( $db_version =~ m/oracle/i ) {
                $logger->log_warning(
                    "Unable to determine dependencies for " . $table_name . " (" . lc($table_type) . ")" );
                $temp{$table_name}{dependency_error} = "Unable to determine dependencies for " . $table_name . ".";
            }
        }

        if ( exists $objects->{'DEPENDENT'} && exists $objects->{'DEPENDENT'}{$table_name} ) {
            my @dependents = @{ $objects->{'DEPENDENT'}{$table_name} };
            foreach my $dependent (@dependents) {
                my $referenced_schema = $dependent->{referenced_schema};
                my $referenced_name   = $dependent->{referenced_name};
                my $referenced_type   = $dependent->{referenced_type};
                # TODO : We really don't need to see dependent triggers on the parent table. Other tables, yes; parent table, no.
                push @{ $temp{$table_name}{dependents} }, [ $referenced_schema, $referenced_name, $referenced_type ];
            }
        }
    }

    # constraints {Check, Primary key, Unique}
    foreach my $table_name ( sort keys %{ $objects->{'CHECK CONSTRAINT'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'CHECK CONSTRAINT'}{$table_name} } ) {
            my $search_condition = $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{search_condition};
            my $status           = $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{status};
            my $comments         = format_comment( $objects->{'CHECK CONSTRAINT'}{$table_name}{$cons_name}{comments} );
            unless ($search_condition) {
                $logger->log_warning("No search condition found for $table_name ($cons_name) check constraint");
                next;
            }
            next if ( $search_condition =~ m/^"[^"]+" IS NOT NULL/i );
            push @{ $temp{$table_name}{constraints} },
                [ $cons_name, 'Check', undef, $search_condition, $status, $comments ];
        }
    }
    foreach my $table_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'UNIQUE CONSTRAINT'}{$table_name} } ) {
            next unless ( exists $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{column_names} );
            my $comments     = format_comment( $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{comments} );
            my $status       = $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{status};
            my $column_names = join( ',', @{ $objects->{'UNIQUE CONSTRAINT'}{$table_name}{$cons_name}{column_names} } );
            push @{ $temp{$table_name}{constraints} },
                [ $cons_name, 'Unique', $column_names, undef, $status, $comments ];
        }
    }
    foreach my $table_name ( sort keys %{ $objects->{'PRIMARY KEY'} } ) {
        next unless ( exists $objects->{'PRIMARY KEY'}{$table_name}{column_names} );
        my $cons_name    = $objects->{'PRIMARY KEY'}{$table_name}{constraint_name};
        my $comments     = format_comment( $objects->{'PRIMARY KEY'}{$table_name}{comments} );
        my $status       = $objects->{'PRIMARY KEY'}{$table_name}{status};
        my $column_names = join( ',', @{ $objects->{'PRIMARY KEY'}{$table_name}{column_names} } );
        push @{ $temp{$table_name}{constraints} },
            [ $cons_name, 'Primary key', $column_names, undef, $status, $comments ];
    }

    # indices
    my %idx;
    foreach my $table_name ( sort keys %{ $objects->{'INDEX'} } ) {
        foreach my $index_name ( sort keys %{ $objects->{'INDEX'}{$table_name} } ) {
            next unless ( exists $objects->{'INDEX'}{$table_name}{$index_name}{column_names} );
            my $is_unique = $objects->{'INDEX'}{$table_name}{$index_name}{is_unique};
            my $comments  = format_comment( $objects->{'INDEX'}{$table_name}{$index_name}{comments} );

            my @columns = @{ $objects->{'INDEX'}{$table_name}{$index_name}{column_names} };
            my @decends = @{ $objects->{'INDEX'}{$table_name}{$index_name}{decends} };
            my @ary     = map { join( ' ', $columns[$_], $decends[$_] ) } ( 0 .. $#columns );
            $_ =~ s/\s+$// for (@ary);
            my $column_names = join( ',', @ary );

            push @{ $temp{$table_name}{indices} }, [ $index_name, $column_names, $is_unique, $comments ];
            my $col_key = join( ',', @columns );
            $idx{$table_name}{$col_key} = 1;
        }
    }

    # foreign keys (parent)
    foreach my $table_name ( sort keys %{ $objects->{'FOREIGN KEY'} } ) {
        foreach my $cons_name ( sort keys %{ $objects->{'FOREIGN KEY'}{$table_name} } ) {
            my $r_table_schema = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_schema};
            my $r_table_name   = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_table_name};
            my $comments       = format_comment( $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{comments} );
            my $cons_rule      = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{constraint_rule};
            #my $status         = $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{status};
            my $column_names   = join( ',', @{ $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{column_names} } );
            my $r_column_names = join( ',', @{ $objects->{'FOREIGN KEY'}{$table_name}{$cons_name}{r_column_names} } );
            my $has_index = ( exists $indices->{$schema_name}{$table_name}{$column_names} ) ? 'Y' : 'N';

            push @{ $temp{$table_name}{parent_keys} },
                [
                $cons_name,      $column_names,
                $r_table_schema, $r_table_name,
                $r_column_names, $cons_rule,
                $comments,       $has_index
                ];
        }
    }

    # foreign keys (child)
    foreach my $table_name ( sort keys %{ $objects->{'CHILD KEY'} } ) {
        foreach my $r_table_schema ( sort keys %{ $objects->{'CHILD KEY'}{$table_name} } ) {
            foreach my $cons_name ( sort keys %{ $objects->{'CHILD KEY'}{$table_name}{$r_table_schema} } ) {

                my $r_table_name = $objects->{'CHILD KEY'}{$table_name}{$r_table_schema}{$cons_name}{r_table_name};
                my $comments =
                    format_comment( $objects->{'CHILD KEY'}{$table_name}{$r_table_schema}{$cons_name}{comments} );
                my $cons_rule = $objects->{'CHILD KEY'}{$table_name}{$r_table_schema}{$cons_name}{constraint_rule};
                #my $status         = $objects->{'CHILD KEY'}{$table_name}{$cons_name}{status};
                my $column_names =
                    join( ',', @{ $objects->{'CHILD KEY'}{$table_name}{$r_table_schema}{$cons_name}{column_names} } );
                my $r_column_names =
                    join( ',', @{ $objects->{'CHILD KEY'}{$table_name}{$r_table_schema}{$cons_name}{r_column_names} } );
                my $has_index = ( exists $indices->{$r_table_schema}{$r_table_name}{$r_column_names} ) ? 'Y' : 'N';

                push @{ $temp{$table_name}{child_keys} },
                    [
                    $cons_name,      $column_names,
                    $r_table_schema, $r_table_name,
                    $r_column_names, $cons_rule,
                    $comments,       $has_index
                    ];
            }
        }
    }

    # Foreign data wrappers
    foreach my $table_name ( sort keys %{ $objects->{'FOREIGN DATA WRAPPER'} } ) {
        my $fdw_name     = $objects->{'FOREIGN DATA WRAPPER'}{$table_name}{fdw_name};
        my $srv_name     = $objects->{'FOREIGN DATA WRAPPER'}{$table_name}{srv_name};
        my $fdw_options  = $objects->{'FOREIGN DATA WRAPPER'}{$table_name}{fdw_options};
        my $fdw_comments = format_comment( $objects->{'FOREIGN DATA WRAPPER'}{$table_name}{comments} );

        push @{ $temp{$table_name}{foreign_wrappers} }, [ $fdw_name, $srv_name, $fdw_options, $fdw_comments ];
    }

    my @optionals = (qw(constraints indices child_keys parent_keys query dependencies dependents foreign_wrappers));
    foreach my $table_name ( keys %temp ) {
        $tt_vars{schema_name} = $schema_name;
        $tt_vars{table_name}  = $table_name;

        $tt_vars{$_} = $temp{$table_name}{$_}
            for (qw(table_owner table_comment row_count columns table_type dependency_error));

        foreach my $optional (@optionals) {
            $tt_vars{$optional} = $temp{$table_name}{$optional} if ( exists $temp{$table_name}{$optional} );
        }
        process_template( "table.tt", \%tt_vars, "$schema_name/tables/$table_name.html" );
        delete $tt_vars{$_} for (@optionals);
        delete $tt_vars{$_} for (qw( schema_name table_name ));
    }
}

sub get_schemas {
    my %schemas;
    my %available_schemas = $extractor->get_objects('SCHEMA');
    if ( $config->get_value('schemas') ) {
        my @requested = split /\s*,\s*/, $config->get_value('schemas');
        foreach my $schema (@requested) {
            $schemas{$schema} = $available_schemas{$schema} if ( exists $available_schemas{$schema} );
        }
    }
    elsif ( $config->get_value('exclude_schemas') ) {
        my %excluded = map { $_ => 1 } ( split /\s*,\s*/, $config->get_value('exclude_schemas') );
        foreach my $schema ( keys %available_schemas ) {
            $schemas{$schema} = $available_schemas{$schema} unless ( exists $excluded{$schema} );
        }
    }
    else {
        %schemas = %available_schemas;
    }
    return %schemas;
}

sub process_template {
    my ( $template_file, $vars, $target_file ) = @_;

    my $template_path = $util->rel_path( $config->{template_dir} );
    my $target_path = $util->rel_path( $config->{target_dir}, $config->{database_name}, $target_file );

    my ( undef, $target_dir, undef ) = File::Spec->splitpath( File::Spec->rel2abs($target_path) );
    $util->mkpath($target_dir);

    my $tt = Template->new( INCLUDE_PATH => $template_path, ) || die "$Template::ERROR\n";
    $tt->process( $template_file, $vars, $target_path ) || die $tt->error(), "\n";

}

sub write_file {
    my ( $target_file, $text ) = @_;

    my $target_path = $util->rel_path( $config->{target_dir}, $config->{database_name}, $target_file );

    my ( undef, $target_dir, undef ) = File::Spec->splitpath( File::Spec->rel2abs($target_path) );
    $util->mkpath($target_dir);

    my $writer = DataDict::Writer->new_writer( 'file' => $target_path );
    $writer->write($text);
    $writer->close();
}

sub format_comment {
    my ($comment) = @_;

    if ($comment) {
        # Use markdown if configured to do so. If not then we need to escape
        # the html here rather than in the template (as before).
        if ($formatter) {
            $comment = $formatter->markdown($comment);
        }
        else {
            $comment =~ s/&/&amp;/g;
            $comment =~ s/</&lt;/g;
            $comment =~ s/>/&gt;/g;
            $comment =~ s/"/&quot;/g;
            $comment = '<p>' . $comment . '</p>';
        }
    }

    return $comment;
}

1;
__END__
