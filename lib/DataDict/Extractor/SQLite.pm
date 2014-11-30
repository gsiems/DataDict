package DataDict::Extractor::SQLite;
use base 'DataDict::Extractor';

use strict;
use warnings;
use Data::Dumper;

sub _post_init {
    my ($self) = @_;

    # my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","");

}

sub _post_set_schema {
    my ($self) = @_;
    if ( exists $self->{table_cache} ) {
        delete $self->{table_cache};
    }
}

sub extract_data {
    my ( $self, %args ) = @_;
    $self->{logger}->log_info("Extracting data ...");
    my $schema         = $self->{schema};
    my $table_name     = $args{table_name};
    my $where_clause   = $args{where_clause} || '';
    my $order_by       = $args{order_by} || '';
    my $exclude_binary = $args{exclude_binary} || 0;

    my %filter = ( IN => [$table_name], );
    # filter columns {all, excluded?, non-blob?}
    # for csv, tab, inserts, etc. lobs probably won't work

    my %columns      = $self->get_table_columns( \%filter );
    my @column_names = @{ $columns{$table_name}{column_names} };
    my @data_types   = @{ $columns{$table_name}{data_types} };

    my $has_lob = 0;
    my @ary;
    my @dt;
    my @cols;
    foreach my $idx ( 0 .. $#column_names ) {
        my $column_name   = $self->{dbh}->quote_identifier( $column_names[$idx] );
        my $data_type     = $data_types[$idx];
        my $column_clause = undef;

        if ( $data_type =~ /^(datetime)/i ) {
            $column_clause = qq{datetime($q_column_name) AS $column_name};
        }
        elsif ( $data_type =~ /(blob)$/i ) {
            unless ($exclude_binary) {
                $has_lob       = 1;
                $column_clause = $column_name;
            }
        }
        else {
            $column_clause = $column_name;
        }

        if ($column_clause) {
            push @ary,  $column_clause;
            push @dt,   $data_type;
            push @cols, $column_names[$idx];
        }
    }
    my $schema_table = $self->{dbh}->quote_identifier( $schema, $table_name );
    my $str = "SELECT ";
    $str .= join( ",\n", @ary );
    $str .= "FROM $schema_table\n";
    $str .= "$where_clause\n" if ($where_clause);
    $str .= "$order_by\n"     if ($order_by);

    my $sth = $self->_db_prepare($str);

    $sth->execute();
    if ( $sth->errstr ) {
        $self->_log_fatal( $sth->errstr );
    }
    return ( $sth, \@cols, \@dt );
}

sub extract_done {
    my ( $self, $sth ) = @_;
    $self->{logger}->log_debug("Finished extracting data...");
    $sth->finish() if ($sth);
}

sub get_objects {
    my ( $self, $object_type, @args ) = @_;

    $object_type ||= 'UNKNOWN';

    if ( uc $object_type eq 'CHECK CONSTRAINT' ) { return $self->get_check_constraints(@args); }
    if ( uc $object_type eq 'CHILD KEY' )        { return $self->get_child_keys(@args); }
    if ( uc $object_type eq 'COLUMN' )           { return $self->get_table_columns(@args); }
    #    if (uc $object_type eq 'DEPENDENCY')        { return $self->get_dependencies(@args);        }
    #    if (uc $object_type eq 'DEPENDENT')         { return $self->get_dependents(@args);          }
    if ( uc $object_type eq 'FOREIGN KEY' ) { return $self->get_foreign_keys(@args); }
    if ( uc $object_type eq 'INDEX' )       { return $self->get_indexes(@args); }
    if ( uc $object_type eq 'PRIMARY KEY' ) { return $self->get_primary_keys(@args); }
    if ( uc $object_type eq 'SCHEMA' )      { return $self->get_schemas(@args); }
    if ( uc $object_type eq 'TABLE' )       { return $self->get_tables(@args); }
    #    if (uc $object_type eq 'TYPE')              { return $self->get_types(@args);               }
    if ( uc $object_type eq 'UNIQUE CONSTRAINT' ) { return $self->get_unique_constraints(@args); }

    # A set of SQL constraints for each table. SQLite supports UNIQUE, NOT NULL, CHECK and FOREIGN KEY constraints.
    # INDEX, TABLE, TRIGGER, VIEW

    $self->{logger}->log_error("No extraction routine available for $object_type objects...");

    return undef;
}

sub _parse_table_ddl {
    my ( $self, $schema, $table_name ) = @_;

    return undef unless ( 'TABLE' eq $self->{table_cache}{$table_name}{table_type} );

    my %return;

    my $schema_table = $self->{dbh}->quote_identifier( $schema, $table_name );
    my $sth          = $self->{dbh}->prepare(qq{select * from $schema_table});
    my @column_names = @{ $sth->{NAME} };
    $return{column_names} = \@column_names;
    my $ddl = $self->{table_cache}{$table_name}{ddl};

    # Split the individual column clauses out from the DDL.
    my %columns;
    my $prev_column = '';
    my ( $clause, $column );
    foreach my $column_name (@column_names) {
        my $quoted_column_name = $self->{dbh}->quote_identifier($column_name);
        my $re                 = qq'($column_name|$quoted_column_name)';
        ( $clause, $column, $ddl ) = split /$re/, $ddl, 2;
        $columns{$prev_column}{clause} = $clause;
        $prev_column = $column_name;
    }
    # Don't want the closing parens
    $ddl =~ s/(\)[^)]*)$//;

    # Now... split out any table constraints that may be tagging along
    # with the last column clause (the remaining DDL).
    # But don't split out any column constraints...

    # Seems as though the first comma that isn't enclosed in a string or between parens qualifies.
    # Split on parens
    # Split on "'" but not "''"  -- single quotes in strings
    # Split on '"' but not '\"'  -- double quotes in identifiers
    my $re = q{((?<!\\\)["]|(?<!')[']|[()])};    # Negative look behind
    my ( $last_column_clause, $table_clause ) = ( '', '' );
    my ( $paren_count, $sq_count, $dq_count ) = ( 0, 0, 0 );

    foreach my $token ( split /$re/, $ddl ) {
        if ($table_clause) {
            $table_clause .= $token;
        }
        elsif ( $token =~ m/,/ && !$paren_count && !$sq_count && !$dq_count ) {
            my @ary = split /(,)/, $token, 2;
            $last_column_clause .= $ary[0] . $ary[1];
            $table_clause = $ary[2];
        }
        else {
            $last_column_clause .= $token;
        }

        if ( $token eq '(' ) {
            $paren_count++ unless ( $sq_count || $dq_count );
        }
        elsif ( $token eq ')' ) {
            $paren_count-- unless ( $sq_count || $dq_count );
        }
        elsif ( $token eq "'" ) {
            $sq_count = !$sq_count unless ( $paren_count || $dq_count );
        }
        elsif ( $token eq '"' ) {
            $dq_count = !$dq_count unless ( $paren_count || $sq_count );
        }
    }

    # ... and ensure that the last column gets it's clause
    $columns{ $column_names[-1] }{clause} = $last_column_clause;

    #print Dumper \%columns;

    # Column constraints: column_name [type_name [(p[,s]]] [constraint [constraint]]
    #  [CONSTRAINT constraint_name]
    #     ( PRIMARY KEY [ASC|DESC] [ON CONFLICT (ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)][AUTOINCREMENT]
    #     | NOT NULL [ON CONFLICT (ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)]
    #     | UNIQUE [ON CONFLICT (ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)]
    #     | CHECK (expr)
    #     | DEFAULT (NUMBER|STRING|(expr))
    #     | COLLATE collate_name
    #     | FOREIGN KEY REFERENCES table_name ...
    #     )
    my $cre = q{(PRIMARY\s+KEY|NOT\s+NULL|UNIQUE|CHECK|DEFAULT|COLLATE|FOREIGN\s+KEY)};
    foreach my $column_name ( keys %columns ) {
        my @ary = grep { $_ } split /\b$cre\b/i, $columns{$column_name}{clause};
        while (@ary) {
            my $token = shift @ary;
            next unless ($token);
            if ( $token =~ m/$cre/i ) {
                my $constraint = $token;
                if ( $ary[0] ) {
                    unless ( $ary[0] =~ m/$cre/i ) {
                        $constraint .= shift @ary;
                    }
                }
                $constraint =~ s/^[\n\s]+//;
                push @{ $columns{$column_name}{constraints} }, $constraint;
            }
        }
    }

    $return{columns} = \%columns;

    # Now let's split the table clauses
    # Table constraints
    #  [CONSTRAINT constraint_name]
    #     ( PRIMARY KEY (column[,column[,...]]) [ON CONFLICT (ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)]
    #     | UNIQUE (column[,column[,...]]) [ON CONFLICT (ROLLBACK|ABORT|FAIL|IGNORE|REPLACE)]
    #     | CHECK (expr)
    #     | FOREIGN KEY (column[,column[,...]]) REFERENCES table_name [(column[,column[,...]])] ...
    #     )
    my @tab_cons_clauses;
    if ($table_clause) {
        my ( $paren_count, $sq_count, $dq_count ) = ( 0, 0, 0 );
        push @tab_cons_clauses, '';
        foreach my $token ( split /$re/, $table_clause ) {
            if ( $token =~ m/,/ && !$paren_count && !$sq_count && !$dq_count ) {
                my @ary = split /(,)/, $token, 2;
                $tab_cons_clauses[-1] .= $ary[0] . $ary[1];
                push @tab_cons_clauses, $ary[2];
            }
            else {
                $tab_cons_clauses[-1] .= $token;
            }

            if ( $token eq '(' ) {
                $paren_count++ unless ( $sq_count || $dq_count );
            }
            elsif ( $token eq ')' ) {
                $paren_count-- unless ( $sq_count || $dq_count );
            }
            elsif ( $token eq "'" ) {
                $sq_count = !$sq_count unless ( $paren_count || $dq_count );
            }
            elsif ( $token eq '"' ) {
                $dq_count = !$dq_count unless ( $paren_count || $sq_count );
            }
        }
        foreach (@tab_cons_clauses) {
            $_ =~ s/^[\n\s]+//;
        }
        $return{table_constraints} = \@tab_cons_clauses;
    }
    return wantarray ? %return : \%return;
}

sub get_check_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Extracting check_constraint information...");
    my %return;
    my $schema = $self->{schema};
    my %tables = $self->get_tables($filters);
    my $name_idx = 1;    # "un-named" constraint name index-- SQLite doesn't require constraint names

    foreach my $table_name ( keys %tables ) {
        next unless ( 'TABLE' eq $tables{$table_name}{table_type} );
        my %pt = $self->_parse_table_ddl( $schema, $table_name );

        foreach my $constraint ( @{ $pt{table_constraints} } ) {
            next unless ( $constraint =~ m/\b(CHECK)\b/i );
            my @ary = grep { $_ } split /\b(CONSTRAINT|CHECK)\b/i, $constraint;
            my ( $constraint_name, $search_condition );

            while (@ary) {
                my $token = shift @ary;
                next unless ($token);
                if ( uc $token eq 'CONSTRAINT' ) {
                    $constraint_name = shift @ary;
                    $constraint_name =~ s/^\s+//;
                    $constraint_name =~ s/\s+$//;
                }
                elsif ( uc $token eq 'CHECK' ) {
                    $search_condition = shift @ary;
                    $search_condition =~ s/^[\s]+//;
                    $search_condition =~ s/[\s]+$//;
                }
            }
            $constraint_name ||= 'unnamed_check_' . $name_idx++;

            $return{$table_name}{$constraint_name}{table_schema}     = $schema;
            $return{$table_name}{$constraint_name}{table_name}       = $table_name;
            $return{$table_name}{$constraint_name}{constraint_name}  = $constraint_name;
            $return{$table_name}{$constraint_name}{search_condition} = $search_condition;
            $return{$table_name}{$constraint_name}{status}           = 'Enabled';
            $return{$table_name}{$constraint_name}{comments}         = '';
        }

        # Column:
        foreach my $column_name ( @{ $pt{column_names} } ) {
            foreach my $constraint ( @{ $pt{columns}{$column_name}{constraints} } ) {
                next unless ( $constraint =~ m/\b(CHECK)\b/i );
                my @ary = grep { $_ } split /\b(CONSTRAINT|CHECK)\b/i, $constraint;
                my ( $constraint_name, $search_condition );
                while (@ary) {
                    my $token = shift @ary;
                    next unless ($token);

                    if ( uc $token eq 'CONSTRAINT' ) {
                        $constraint_name = shift @ary;
                        $constraint_name =~ s/^\s+//;
                        $constraint_name =~ s/\s+$//;
                    }
                    elsif ( uc $token eq 'CHECK' ) {
                        $search_condition = shift @ary;
                        $search_condition =~ s/^[\s]+//;
                        $search_condition =~ s/[\s]+$//;
                    }
                }
                $constraint_name ||= 'unnamed_check_' . $name_idx++;

                $return{$table_name}{$constraint_name}{table_schema}     = $schema;
                $return{$table_name}{$constraint_name}{table_name}       = $table_name;
                $return{$table_name}{$constraint_name}{constraint_name}  = $constraint_name;
                $return{$table_name}{$constraint_name}{search_condition} = $search_condition;
                $return{$table_name}{$constraint_name}{status}           = 'Enabled';
                $return{$table_name}{$constraint_name}{comments}         = '';
            }
        }
    }
    return wantarray ? %return : \%return;
}

sub get_child_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving child key information...");
    my %return;
    my $schema = $self->{schema};

    my %fk = $self->get_foreign_keys($filters);

    foreach my $table_name ( keys %fk ) {

        # find all fks that have $table_name as the $r_table_name
        foreach my $constraint_name ( keys %{ $fk{$table_name} } ) {
            my $r_table_name   = $fk{$table_name}{$constraint_name}{r_table_name};
            my $r_table_schema = $fk{$table_name}{$constraint_name}{r_table_schema};
            my $table_schema   = $fk{$table_name}{$constraint_name}{table_schema};
            # TODO : If you attach files to an SQLite database,
            # can you create foreign keys between schemas?
            # If so then we need to query and cache all schemas ...

            next unless ( $schema eq $r_table_schema );

            $return{$r_table_name}{$table_schema}{$constraint_name}{table_schema}   = $r_table_schema;
            $return{$r_table_name}{$table_schema}{$constraint_name}{table_name}     = $r_table_name;
            $return{$r_table_name}{$table_schema}{$constraint_name}{r_table_schema} = $schema;
            $return{$r_table_name}{$table_schema}{$constraint_name}{r_table_name}   = $table_name;

            $return{$r_table_name}{$table_schema}{$constraint_name}{r_column_names} =
                $fk{$table_name}{$table_schema}{$constraint_name}{column_names};

            $return{$r_table_name}{$table_schema}{$constraint_name}{column_names} =
                $fk{$table_name}{$table_schema}{$constraint_name}{r_column_names};

            $return{$r_table_name}{$table_schema}{$constraint_name}{constraint_rule} =
                $fk{$table_name}{$table_schema}{$constraint_name}{constraint_rule};

            $return{$r_table_name}{$table_schema}{$constraint_name}{status} =
                $fk{$table_name}{$table_schema}{$constraint_name}{status};
            $return{$r_table_name}{$table_schema}{$constraint_name}{comments} =
                $fk{$table_name}{$table_schema}{$constraint_name}{comments};
        }
    }
    return wantarray ? %return : \%return;
}

sub get_db_version {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database version information...");
    my $version = 'SQLite version ' . $self->{dbh}->{sqlite_version};
    return $version;
}

sub get_foreign_keys {
    my ( $self, $filters ) = @_;

=pod

TODO use foreign_key_list ???

AFAICT SQLite's foreign_key_list($table_name) pragma returns a list of 8 elements in this order (i.e. with these interpretations):

1: COUNT
2: KEY_SEQ
3: FKTABLE_NAME
4: PKCOLUMN_NAME
5: FKCOLUMN_NAME
6: UPDATE_RULE
7: DELETE_RULE
8: UNKNOWN

If anyone can explain the last item, or offer corrections, please advise.

    2 comments

2 Comments
Author Profile Page Mr. Muskrat | March 13, 2013 5:53 AM | Reply

According to Using SQLite by Jay A. Kreibich the elements are:
id Integer "Foreign key ID number"
seq Integer "Column sequence number for this key"
table Text "Name of foreign table"
from Text "Local column name"
to Text "Foreign column name"
on_update Text "ON UPDATE action"
on_delete Text "ON DELETE action"
match Text "Always NONE"

=cut

    $self->{logger}->log_info("Retrieving foreign key information...");
    my %return;
    my $schema = $self->{schema};
    my %tables = $self->get_tables($filters);
    my $name_idx = 1;    # "un-named" constraint name index-- SQLite doesn't require constraint names

    foreach my $table_name ( keys %tables ) {
        next unless ( 'TABLE' eq $tables{$table_name}{table_type} );
        my %pt = $self->_parse_table_ddl( $schema, $table_name );
        my %pk = $self->get_primary_keys($filters);

        foreach my $constraint ( @{ $pt{table_constraints} } ) {
            next unless ( $constraint =~ m/\b(FOREIGN\s+KEY)\b/i );
            my @ary = grep { $_ } split /\b(CONSTRAINT|FOREIGN\s+KEY|REFERENCES|ON|MATCH|(NOT)*\s+DEFERRABLE)\b/i,
                $constraint;
            my ( $constraint_name, $column_names, $r_table_schema, $r_table_name, $r_column_names, $constraint_rule );

            while (@ary) {
                my $token = shift @ary;
                next unless ($token);
                if ( uc $token eq 'CONSTRAINT' ) {
                    $constraint_name = shift @ary;
                    $constraint_name =~ s/^\s+//;
                    $constraint_name =~ s/\s+$//;
                }
                elsif ( $token =~ m/FOREIGN\s+KEY/i ) {
                    $column_names = shift @ary;
                    $column_names =~ s/^[\s(]+//;
                    $column_names =~ s/[\s)]+$//;
                }
                elsif ( uc $token eq 'REFERENCES' ) {
                    my $reference = shift @ary;
                    ( $r_table_name, $r_column_names ) = split /\s*[()]\s*/, $reference;
                    if ( $r_table_name =~ m/\./ ) {
                        ( $r_table_schema, $r_table_name ) = split /\./, $r_table_name;
                    }
                    $r_table_name =~ s/^[\s"]+//;
                    $r_table_name =~ s/[\s"]+$//;

                    $r_table_schema ||= $schema;
                    $r_table_schema =~ s/^[\s"]+//;
                    $r_table_schema =~ s/[\s"]+$//;

                    unless ($r_column_names) {
                        # If no columns specified then PK of referenced table
                        if ( $schema eq $r_table_schema ) {
                            $r_column_names = join( ',', $pk{$table_name}{column_names} );
                        }
                    }
                }
                elsif ( uc $token eq 'ON' ) {
                    $constraint_rule .= $token . shift @ary;
                }
            }
            $constraint_name ||= 'un_named_fk_' . $name_idx++;

            $return{$table_name}{$constraint_name}{table_schema}    = $schema;
            $return{$table_name}{$constraint_name}{table_name}      = $table_name;
            $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
            @{ $return{$table_name}{$constraint_name}{column_names} } = split /\s*,\s*/, $column_names;
            $return{$table_name}{$constraint_name}{r_table_schema} = $r_table_schema;
            $return{$table_name}{$constraint_name}{r_table_name}   = $r_table_name;
            @{ $return{$table_name}{$constraint_name}{r_column_names} } = split /\s*,\s*/, $r_column_names;
            $return{$table_name}{$constraint_name}{constraint_rule} = $constraint_rule;
            $return{$table_name}{$constraint_name}{status}          = 'Enabled';
            $return{$table_name}{$constraint_name}{comments}        = '';
        }

        foreach my $column_name ( @{ $pt{column_names} } ) {
            foreach my $constraint ( @{ $pt{columns}{$column_name}{constraints} } ) {
                next unless ( $constraint =~ m/\bUNIQUE[\s\n(]/i );

                next unless ( $constraint =~ m/\b(FOREIGN\s+KEY|REFERENCES)\b/i );
                my @ary = grep { $_ } split /\b(CONSTRAINT|FOREIGN\s+KEY|REFERENCES|ON|MATCH|(NOT)*\s+DEFERRABLE)\b/i,
                    $constraint;
                my (
                    $constraint_name, $column_names,
                    $r_table_schema,  $r_table_name,
                    $r_column_names,  $constraint_rule
                );
                $column_names = $column_name;

                while (@ary) {
                    my $token = shift @ary;
                    next unless ($token);
                    if ( uc $token eq 'CONSTRAINT' ) {
                        $constraint_name = shift @ary;
                        $constraint_name =~ s/^\s+//;
                        $constraint_name =~ s/\s+$//;
                    }
                    elsif ( uc $token eq 'REFERENCES' ) {
                        my $reference = shift @ary;
                        ( $r_table_name, $r_column_names ) = split /\s*[()]\s*/, $reference;
                        if ( $r_table_name =~ m/\./ ) {
                            ( $r_table_schema, $r_table_name ) = split /\./, $r_table_name;
                        }
                        $r_table_name =~ s/^[\s"]+//;
                        $r_table_name =~ s/[\s"]+$//;

                        $r_table_schema ||= $schema;
                        $r_table_schema =~ s/^[\s"]+//;
                        $r_table_schema =~ s/[\s"]+$//;

                        unless ($r_column_names) {
                            # If no columns specified then PK of referenced table
                            if ( $schema eq $r_table_schema ) {
                                $r_column_names = join( ',', $pk{$table_name}{column_names} );
                            }
                        }
                    }
                    elsif ( uc $token eq 'ON' ) {
                        $constraint_rule .= $token . shift @ary;
                    }
                }
                $constraint_name ||= 'un_named_fk_' . $name_idx++;

                $return{$table_name}{$constraint_name}{table_schema}    = $schema;
                $return{$table_name}{$constraint_name}{table_name}      = $table_name;
                $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
                @{ $return{$table_name}{$constraint_name}{column_names} } = ($column_name);
                $return{$table_name}{$constraint_name}{r_table_schema} = $r_table_schema;
                $return{$table_name}{$constraint_name}{r_table_name}   = $r_table_name;
                @{ $return{$table_name}{$constraint_name}{r_column_names} } = split /\s*,\s*/, $r_column_names;
                $return{$table_name}{$constraint_name}{constraint_rule} = $constraint_rule;
                $return{$table_name}{$constraint_name}{status}          = 'Enabled';
                $return{$table_name}{$constraint_name}{comments}        = '';
            }
        }

        # REFERENCES table_name ... Hmmm
    }
    return wantarray ? %return : \%return;
}

sub get_indexes {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving index information...");
    my %return;
    my $schema = $self->{schema};
    my %filter = $self->_get_table_filter($filters);

    my $sth = $self->{dbh}->table_info( undef, $schema, undef, 'INDEX' );
    foreach my $row ( @{ $sth->fetchall_arrayref() } ) {
        my $table_name = $row->[2];
        my $ddl        = $row->[5];
        next if ( exists $filter{'IN'}     && !exists $filter{'IN'}{$table_name} );
        next if ( exists $filter{'NOT IN'} && exists $filter{'NOT IN'}{$table_name} );
        next unless ($ddl);

        my $is_unique = ( $ddl =~ m/UNIQUE INDEX/i ) ? 'Y' : 'N';
        my ($index_name) = $ddl =~ m/\sINDEX\b(.+)\sON\s/i;

        $index_name =~ s/^\s+//;
        $index_name =~ s/\s+$//;

        $return{$table_name}{$index_name}{table_schema} = $row->[1];
        $return{$table_name}{$index_name}{table_name}   = $row->[2];
        $return{$table_name}{$index_name}{index_name}   = $index_name;
        $return{$table_name}{$index_name}{is_unique}    = $is_unique;
        $return{$table_name}{$index_name}{comments}     = '';

        my ($column_list) = $ddl =~ m/\((.+)\)/;
        foreach my $column ( split /\s*,\s*/, $column_list ) {
            my ( $column_name, $descends ) = split /\s+/, $column;
            push @{ $return{$table_name}{$index_name}{column_names} }, $column_name;
            push @{ $return{$table_name}{$index_name}{decends} }, $descends || '';
        }
    }
    return wantarray ? %return : \%return;
}

sub get_primary_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving primary key information...");
    my %return;
    my $schema = $self->{schema};
    my %tables = $self->get_tables($filters);

    foreach my $table_name ( keys %tables ) {
        next unless ( 'TABLE' eq $tables{$table_name}{table_type} );

        my @key_columns = $self->{dbh}->primary_key( undef, $schema, $table_name );
        if (@key_columns) {
            $return{$table_name}{table_schema}    = $schema;
            $return{$table_name}{table_name}      = $table_name;
            $return{$table_name}{constraint_name} = '';
            $return{$table_name}{column_names}    = \@key_columns;
            $return{$table_name}{status}          = 'Enabled';
            $return{$table_name}{comments}        = '';
        }
    }
    return wantarray ? %return : \%return;
}

sub get_schemas {
    my ( $self, @schemas ) = @_;
    $self->{logger}->log_info("Retrieving schema information...");
    my %return;

    # TODO : schema name filter

    my $sth  = $self->{dbh}->table_info();
    my $rows = $sth->fetchall_arrayref();

    foreach my $row ( @{$rows} ) {
        my $catalog_name = $row->[0];
        my $schema_name  = $row->[1];
        next if ( exists $return{$schema_name} );
        $return{$schema_name}{catalog_name} = $catalog_name;
        $return{$schema_name}{schema_name}  = $schema_name;
        $return{$schema_name}{schema_owner} = undef;
        $return{$schema_name}{comments}     = undef;
    }
    return wantarray ? %return : \%return;
}

sub get_table_columns {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_debug("Extracting table column information ...");
    my %return;
    my $schema = $self->{schema};
    my %tables = $self->get_tables($filters);

    foreach my $table_name ( keys %tables ) {
        my $sth2        = $self->{dbh}->prepare(qq{select * from "$schema"."$table_name"});
        my @tab_columns = @{ $sth2->{NAME} };
        my @data_types  = @{ $sth2->{TYPE} };
        $sth2->finish();

        my $sth3 = $self->{dbh}->column_info( undef, $schema, $table_name, $tab_columns[0] );
        my @meta_columns = @{ $sth3->{NAME_lc} };
        $sth3->finish();
        my %mc = map { $meta_columns[$_] => $_ } ( 0 .. $#meta_columns );

        foreach my $idx ( 0 .. $#tab_columns ) {
            my $col         = $tab_columns[$idx];
            my $sth4        = $self->{dbh}->column_info( undef, $schema, $table_name, $col );
            my @column_meta = $sth4->fetchrow_array;
            my $column_name = $column_meta[ $mc{column_name} ];

            $return{$table_name}{columns}{$column_name}{table_schema}     = $schema;
            $return{$table_name}{columns}{$column_name}{table_name}       = $table_name;
            $return{$table_name}{columns}{$column_name}{column_name}      = $column_name;
            $return{$table_name}{columns}{$column_name}{ordinal_position} = $column_meta[ $mc{ordinal_position} ];
            $return{$table_name}{columns}{$column_name}{data_type}        = $column_meta[ $mc{type_name} ];
            $return{$table_name}{columns}{$column_name}{data_precision}   = $column_meta[ $mc{column_size} ];
            $return{$table_name}{columns}{$column_name}{data_scale}       = $column_meta[ $mc{decimal_digits} ];
            ( $return{$table_name}{columns}{$column_name}{is_nullable} ) = $column_meta[ $mc{is_nullable} ] =~ m/^(.)/;
            $return{$table_name}{columns}{$column_name}{data_default} = $column_meta[ $mc{column_def} ];
            $return{$table_name}{columns}{$column_name}{comments}     = $column_meta[ $mc{remarks} ];

            push @{ $return{$table_name}{column_names} }, $column_name;
            push @{ $return{$table_name}{data_types} },   $data_types[$idx];
        }
    }
    return wantarray ? %return : \%return;
}

sub get_tables {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table information...");

    if ( exists $self->{table_cache} ) {
        return wantarray ? %{ $self->{table_cache} } : $self->{table_cache};
    }

    my %return;
    my $schema = $self->{schema};
    my %filter = $self->_get_table_filter($filters);

    foreach my $table_type (qw(TABLE VIEW)) {
        my $sth = $self->{dbh}->table_info( undef, $schema, undef, $table_type );

        foreach my $row ( @{ $sth->fetchall_arrayref() } ) {
            my $table_name = $row->[2];
            next if ( exists $filter{'IN'}     && !exists $filter{'IN'}{$table_name} );
            next if ( exists $filter{'NOT IN'} && exists $filter{'NOT IN'}{$table_name} );

            $return{$table_name}{$_} = undef for (qw( table_owner tablespace_name row_count comments ));
            $return{$table_name}{catalog_name} = $row->[0];
            $return{$table_name}{table_schema} = $row->[1];
            $return{$table_name}{table_name}   = $row->[2];
            $return{$table_name}{table_type}   = $row->[3];
            $return{$table_name}{query}        = $row->[5] if ( 'VIEW' eq $table_type );
            $return{$table_name}{ddl}          = $row->[5];

            my $sth2 = $self->{dbh}->prepare(qq{select * from "$schema"."$table_name"});
            $return{$table_name}{column_count} = scalar @{ $sth2->{NAME} };
        }
    }
    $self->{table_cache} = \%return;
    return wantarray ? %return : \%return;
}

sub get_unique_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving unique constraint information...");
    my %return;
    my $schema = $self->{schema};
    my %tables = $self->get_tables($filters);
    my $name_idx = 1;    # "un-named" constraint name index-- SQLite doesn't require constraint names

    foreach my $table_name ( keys %tables ) {
        next unless ( 'TABLE' eq $tables{$table_name}{table_type} );
        my %pt = $self->_parse_table_ddl( $schema, $table_name );

        foreach my $constraint ( @{ $pt{table_constraints} } ) {
            next unless ( $constraint =~ m/\b(UNIQUE)\b]/i );
            my @ary = grep { $_ } split /\b(UNIQUE)\b/i, $constraint;

            my ( $constraint_name, $column_names );

            #    [CONSTRAINT [constraint_name]] UNIQUE(column[,column[,...]])
            while (@ary) {
                my $token = shift @ary;
                next unless ($token);
                if ( uc $token eq 'CONSTRAINT' ) {
                    $constraint_name = shift @ary;
                    $constraint_name =~ s/^\s+//;
                    $constraint_name =~ s/\s+$//;
                }
                elsif ( uc $token eq 'UNIQUE' ) {
                    $column_names = shift @ary;
                    $column_names =~ s/^[\s(]+//;
                    $column_names =~ s/[\s)]+$//;
                }
            }
            $constraint_name ||= 'un_named_unique_' . $name_idx++;

            $return{$table_name}{$constraint_name}{table_schema}    = $schema;
            $return{$table_name}{$constraint_name}{table_name}      = $table_name;
            $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
            @{ $return{$table_name}{$constraint_name}{column_names} } = split /\s*,\s*/, $column_names;
            $return{$table_name}{$constraint_name}{status}   = 'Enabled';
            $return{$table_name}{$constraint_name}{comments} = '';
        }

        # Column:
        foreach my $column_name ( @{ $pt{column_names} } ) {
            foreach my $constraint ( @{ $pt{columns}{$column_name}{constraints} } ) {
                next unless ( $constraint =~ m/\bUNIQUE[\s\n(]/i );
                my @ary = grep { $_ } split /\b(UNIQUE)\b/i, $constraint;
                my ($constraint_name);

                while (@ary) {
                    my $token = shift @ary;
                    next unless ($token);
                    if ( uc $token eq 'CONSTRAINT' ) {
                        $constraint_name = shift @ary;
                        $constraint_name =~ s/^\s+//;
                        $constraint_name =~ s/\s+$//;
                    }
                }
                $constraint_name ||= 'un_named_unique_' . $name_idx++;

                $return{$table_name}{$constraint_name}{table_schema}    = $schema;
                $return{$table_name}{$constraint_name}{table_name}      = $table_name;
                $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
                @{ $return{$table_name}{$constraint_name}{column_names} } = ($column_name);
                $return{$table_name}{$constraint_name}{status}   = 'Enabled';
                $return{$table_name}{$constraint_name}{comments} = '';
            }
        }
    }
    return wantarray ? %return : \%return;
}

sub _get_table_filter {
    my ( $self, $filters ) = @_;
    my %filter;
    foreach my $tag ( 'IN', 'NOT IN' ) {
        if ( exists $filters->{$tag} ) {
            $filter{$tag}{$_} = 1 for ( @{ $filters->{$tag} } );
        }
    }
    return %filter;
}

1;
__END__
