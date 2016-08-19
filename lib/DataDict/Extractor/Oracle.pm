package DataDict::Extractor::Oracle;
use base 'DataDict::Extractor';

use strict;
use warnings;

# TODO: prepare and extract user query as table
# TODO: 'schema_name' vs. 'owner' in result sets

sub _post_init {
    my ($self) = @_;

    # determine which tables 'ALL_*' vs. 'DBA_*' the user is able to query
    my $str = q{
SELECT  table_name
    FROM sys.all_tab_privs
    WHERE ( table_name LIKE 'DBA_%'
            OR table_name IN ( 'V$VERSION', 'NLS_DATABASE_PARAMETERS', 'NLS_SESSION_PARAMETERS' )
            )
        AND privilege = 'SELECT'
};

    $self->{sysuser_list} = "'" . join(
        "','",
        (
            qw( APPQOSSYS CTXSYS DBSNMP DMSYS EXFSYS MDSYS OLAPSYS
                ORACLE_OCM ORDSYS OUTLN PERFSTAT PUBLIC SQLTXPLAIN
                SYS SYSMAN SYSTEM TSMSYS WMSYS XDB )
        )
    ) . "'";

    foreach my $table ( $self->_db_query($str) ) {
        $self->{select_privs}{ lc $table->[0] } = 1;
    }
}

sub _post_set_schema {
    my ($self) = @_;

}

sub _has_select_priv {
    my ( $self, @needs ) = @_;
    foreach my $needed (@needs) {
        return 0 unless ( exists $self->{select_privs}{ lc $needed } );
    }
    return 1;
}

sub compile_schema {
    my ($self) = @_;
    my $schema = $self->{schema};
    $self->{logger}->log_info("Compiling objects in $schema ...");
    my $str = qq{BEGIN
DBMS_UTILITY.COMPILE_SCHEMA ( schema => '$schema' );
END;
};
    $self->_db_do($str);
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

        if ( $data_type =~ /^(date|timestamp)/i ) {
            $column_clause = qq{to_char($column_name, 'YYYY-MM-DD HH24:MI:SS') AS $column_name};
        }
        elsif ( $data_type =~ /^xmltype/i ) {
            $has_lob       = 1;
            $column_clause = qq{$column_name.extract('/').getStringVal() AS $column_name};
        }
        elsif ( $data_type =~ /(blob|raw|bfile)$/i ) {
            unless ($exclude_binary) {
                $has_lob       = 1;
                $column_clause = $column_name;
            }
        }
        elsif ( $data_type =~ /(clob|long)$/i ) {
            $has_lob       = 1;
            $column_clause = $column_name;
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
    $str .= "\nFROM $schema_table\n";
    $str .= "$where_clause\n" if ($where_clause);
    $str .= "$order_by\n"     if ($order_by);

    if ($has_lob) {
        $self->_set_longreadlen();
    }
    else {
        $self->_clear_longreadlen();
    }
    my $sth = $self->_db_prepare($str);

    $sth->execute();
    if ( $sth->errstr ) {
        $self->_log_fatal( $sth->errstr );
    }
    return ( $sth, \@cols, \@dt );
}

sub extract_done {
    my ( $self, $sth ) = @_;
    $self->{logger}->log_debug("Finished extracting data ...");
    $sth->finish() if ($sth);
    $self->_clear_longreadlen();
}

sub get_table_dependency_order {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table dependency order information ...");
    my $schema = $self->{schema};

    my %tables       = $self->get_tables($filters);
    my %foreign_keys = $self->get_foreign_keys($filters);
    my %deps;

    foreach my $table ( keys %tables ) {
        next unless ( $tables{$table}{table_type} eq 'TABLE' );
        $deps{$table}{resolved} = 0;
    }

    #    foreach my $name (keys %dependencies) {
    #        my @ary = @{$dependencies{$name}};
    #        foreach my $d (@ary) {
    #            next unless ($d{schema} eq $d{referenced_schema});
    #            next unless ($d{type}            =~ m/^(TABLE|VIEW|MATERIALIZED VIEW)$/);
    #            next unless ($d{referenced_type} =~ m/^(TABLE|VIEW|MATERIALIZED VIEW)$/);
    #
    #            my $ref = $d{referenced_name};
    #            $deps{$name}{parent}{$ref} = 1;
    #            $deps{$ref}{child}{$name}  = 1
    #        }
    #    }

    foreach my $table ( keys %deps ) {
        next unless ( exists $foreign_keys{$table} );
        foreach my $constraint ( keys %{ $foreign_keys{$table} } ) {
            next
                unless ( $foreign_keys{$table}{$constraint}{table_schema} eq
                $foreign_keys{$table}{$constraint}{r_table_schema} );
            my $ref = $foreign_keys{$table}{$constraint}{r_table_name};
            next if ( $ref eq $table );
            next unless ( exists $deps{$table} && exists $deps{$ref} );
            $deps{$table}{parent}{$ref} = 1;
            $deps{$ref}{child}{$table}  = 1;
        }
    }

    foreach my $table ( sort keys %deps ) {
        my $parents  = join( ', ', ( sort keys %{ $deps{$table}{parent} } ) );
        my $children = join( ', ', ( sort keys %{ $deps{$table}{child} } ) );

        print "$schema.$table Parents  :" . $parents . "\n";
        print "$schema.$table Children :" . $children . "\n";

    }

    my @parents;
    my @children;
    my $resolved = 1;
    my $pass     = 0;
    while ($resolved) {
        $pass++;
        $self->{logger}->log_info("Dependency order resolution pass $pass ...");
        $resolved = 0;
        foreach my $table ( sort keys %deps ) {
            next if ( grep( /^$table$/, @parents, @children ) );

            #            if (exists $deps{$table}) {
            unless ( $deps{$table}{parent} ) {
                # has no parents
                push @parents, $table;
                foreach my $child ( keys %{ $deps{$table}{child} } ) {
                    delete $deps{$child}{parent}{$table};
                    delete $deps{$child}{parent} unless ( keys %{ $deps{$child}{parent} } );
                }
                $resolved++;
                next;
            }
            #                unless ($deps{$table}{child}) {
            #                    # has no children
            #                    unshift @children, $table;
            #                    foreach my $parent (keys %{$deps{$table}{parent}}) {
            #                        delete $deps{$parent}{child}{$table};
            #                    }
            #                    $resolved++;
            #                    next;
            #                }
            #            }
            #            else {
            #                push @parents, $table;
            #            }
        }
    }

    # If there are still unresolved tables then they...
    # ...go in the middle someplace?
    # Possible cyclical references?
    foreach my $table ( sort keys %deps ) {
        next if ( grep( /^$table$/i, @parents, @children ) );
        $self->{logger}->log_warning("Unable to resolve table dependency order for \"$table\" ...");
        push @parents, $table;
    }

    push @parents, $_ for (@children);
    return @parents;
}

sub get_objects {
    my ( $self, $object_type, @args ) = @_;

    $object_type ||= 'UNKNOWN';

    if ( uc $object_type eq 'CHECK CONSTRAINT' )  { return $self->get_check_constraints(@args); }
    if ( uc $object_type eq 'CHILD KEY' )         { return $self->get_child_keys(@args); }
    if ( uc $object_type eq 'COLUMN PRIV' )       { return $self->get_column_privs(@args); }
    if ( uc $object_type eq 'COLUMN' )            { return $self->get_table_columns(@args); }
    if ( uc $object_type eq 'DEPENDENCY' )        { return $self->get_dependencies(@args); }
    if ( uc $object_type eq 'DEPENDENT' )         { return $self->get_dependents(@args); }
    if ( uc $object_type eq 'DOMAIN' )            { return undef; }
    if ( uc $object_type eq 'FOREIGN KEY' )       { return $self->get_foreign_keys(@args); }
    if ( uc $object_type eq 'FUNCTION' )          { return $self->_get_object_source( uc $object_type, @args ); }
    if ( uc $object_type eq 'INDEX' )             { return $self->get_indexes(@args); }
    if ( uc $object_type eq 'PACKAGE' )           { return $self->_get_object_source( uc $object_type, @args ); }
    if ( uc $object_type eq 'PARTITION' )         { return $self->get_partitions(@args); }
    if ( uc $object_type eq 'PRIMARY KEY' )       { return $self->get_primary_keys(@args); }
    if ( uc $object_type eq 'PROCEDURE' )         { return $self->_get_object_source( uc $object_type, @args ); }
    if ( uc $object_type eq 'SCHEMA' )            { return $self->get_schemas(@args); }
    if ( uc $object_type eq 'SEQUENCE' )          { return $self->get_sequences(@args); }
    if ( uc $object_type eq 'TABLE PRIV' )        { return $self->get_table_privs(@args); }
    if ( uc $object_type eq 'TABLE' )             { return $self->get_tables(@args); }
    if ( uc $object_type eq 'TABLESPACE' )        { return $self->get_tablespaces(@args); }
    if ( uc $object_type eq 'TRIGGER' )           { return $self->get_triggers(@args); }
    if ( uc $object_type eq 'TYPE' )              { return $self->_get_object_source( uc $object_type, @args ); }
    if ( uc $object_type eq 'UNIQUE CONSTRAINT' ) { return $self->get_unique_constraints(@args); }

    $self->{logger}->log_error("No extraction routine available for $object_type objects ...");

    return undef;
}

sub get_check_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Extracting check_constraint information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $str = qq{
SELECT owner AS table_schema,
        table_name,
        constraint_name,
        search_condition,
        CASE status
            WHEN 'ENABLED' THEN 'Enabled'
            WHEN 'DISABLED' THEN 'Disabled'
            ELSE status
            END AS status
    FROM sys.all_constraints
    WHERE constraint_type = 'C'
        AND owner = ? $table_filter
};

    my $query = qq{
SELECT column_name,
        position
    FROM sys.all_cons_columns
    WHERE owner = ?
        AND table_name = ?
        AND constraint_name = ?
    ORDER BY position
};

    $self->_set_longreadlen();
    my $sth = $self->_db_prepare($query);

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_schema    = $row->[0];
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        $return{$table_name}{$constraint_name}{table_schema}     = $table_schema;
        $return{$table_name}{$constraint_name}{table_name}       = $table_name;
        $return{$table_name}{$constraint_name}{constraint_name}  = $constraint_name;
        $return{$table_name}{$constraint_name}{search_condition} = $row->[3];
        $return{$table_name}{$constraint_name}{status}           = $row->[4];
        $return{$table_name}{$constraint_name}{comments}         = '';

        $sth->execute( $table_schema, $table_name, $constraint_name ) || $self->_log_fatal( $sth->errstr );
        foreach my $line ( @{ $sth->fetchall_arrayref } ) {
            push @{ $return{$table_name}{$constraint_name}{column_names} },     $line->[0];
            push @{ $return{$table_name}{$constraint_name}{column_positions} }, $line->[1];
        }
    }

    $self->_clear_longreadlen();
    return wantarray ? %return : \%return;
}

sub get_child_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving child key information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r_col.table_name', $filters );

    my $str = qq{
SELECT DISTINCT r_col.owner AS table_schema,
        r_col.table_name,
        con.constraint_name,
        r_col.column_name,
        col.owner AS r_table_schema,
        col.table_name AS r_table_name,
        col.column_name AS r_column_name,
        'ON DELETE ' || con.delete_rule AS constraint_rule,
        CASE con.status
            WHEN 'ENABLED' THEN 'Enabled'
            WHEN 'DISABLED' THEN 'Disabled'
            ELSE con.status
            END AS status,
        '' AS comments
    FROM sys.all_constraints con
    INNER JOIN sys.all_cons_columns col
        ON ( con.owner = col.owner
            AND con.table_name = col.table_name
            AND con.constraint_name = col.constraint_name )
    INNER JOIN sys.all_cons_columns r_col
        ON ( con.r_owner = r_col.owner
            AND con.r_constraint_name = r_col.constraint_name )
    WHERE con.constraint_type = 'R'
        AND col.position = r_col.position
        AND r_col.owner = ? $table_filter
    ORDER BY r_col.owner,
        r_col.table_name,
        con.constraint_name,
        r_col.column_name
};

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_name      = $row->[1];
        my $r_table_schema  = $row->[4];
        my $constraint_name = $row->[2];
        unless ( exists $return{$table_name}{$r_table_schema}{$constraint_name} ) {
            $return{$table_name}{$r_table_schema}{$constraint_name}{table_schema}    = $row->[0];
            $return{$table_name}{$r_table_schema}{$constraint_name}{table_name}      = $table_name;
            $return{$table_name}{$r_table_schema}{$constraint_name}{constraint_name} = $constraint_name;
            $return{$table_name}{$r_table_schema}{$constraint_name}{r_table_schema}  = $row->[4];
            $return{$table_name}{$r_table_schema}{$constraint_name}{r_table_name}    = $row->[5];
            $return{$table_name}{$r_table_schema}{$constraint_name}{constraint_rule} = $row->[7];
            $return{$table_name}{$r_table_schema}{$constraint_name}{status}          = $row->[8];
            $return{$table_name}{$r_table_schema}{$constraint_name}{comments}        = '';
        }
        push @{ $return{$table_name}{$r_table_schema}{$constraint_name}{column_names} },   $row->[3];
        push @{ $return{$table_name}{$r_table_schema}{$constraint_name}{r_column_names} }, $row->[6];
    }
    return wantarray ? %return : \%return;
}

sub get_column_privs {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving column privilege information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    # ASSERTION: Names for tables, views, materialized, functions,
    #   packages, and procedures are unique schema wide.
    my $str = '';
    if ( $self->_has_select_priv('dba_col_privs') ) {
        $str = qq{
SELECT owner AS table_schema,
        table_name,
        column_name,
        grantee,
        privilege
    FROM sys.dba_col_privs
    WHERE owner = ? $table_filter
};
    }
    else {
        $str = qq{
SELECT table_schema,
        table_name,
        column_name,
        grantee,
        privilege
    FROM sys.all_col_privs
    WHERE table_schema = ? $table_filter
};
    }

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $name = $row->[1];
        my %v;
        $v{table_schema} = $row->[0];
        $v{table_name}   = $row->[1];
        $v{column_name}  = $row->[2];
        $v{grantee}      = $row->[3];
        $v{privilege}    = $row->[4];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_db_encoding {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database encoding information ...");

    my $str = q{
SELECT  d.parameter,
        CASE
            WHEN d.parameter = 'NLS_CHARACTERSET' THEN d.value
            ELSE coalesce ( s.value, d.value )
        END AS value
    FROM sys.nls_database_parameters d
    LEFT OUTER JOIN sys.nls_session_parameters s
        ON ( d.parameter = s.parameter )
    WHERE d.parameter IN ( 'NLS_LANGUAGE', 'NLS_TERRITORY', 'NLS_CHARACTERSET' )
};

    my $language  = '';
    my $territory = '';
    my $charset   = '';

    foreach my $row ( $self->_db_query($str) ) {
        if ( $row->[0] eq 'NLS_LANGUAGE' ) {
            $language = $row->[1];
        }
        elsif ( $row->[0] eq 'NLS_TERRITORY' ) {
            $territory = $row->[1];
        }
        elsif ( $row->[0] eq 'NLS_CHARACTERSET' ) {
            $charset = $row->[1];
        }
    }
    my $encoding = $language . '_' . $territory . '.' . $charset;
    return $encoding;
}

sub get_db_version {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database version information ...");

    my $str = q{
SELECT banner
    FROM v$version
    WHERE lower ( banner ) LIKE '%database%'
};

    my ($row) = $self->_db_fetch($str);
    return $row->[0];
}

sub get_dependencies {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving object dependency information ...");
    my %return;
    my $schema       = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'd.name', $filters );
    my $not_in       = $self->{sysuser_list};

    # Oddly enough, all_dependencies won't return dependencies for
    # materialized views whereas dba_dependencies does. Therefore,
    # we want to use dba_dependencies if possible.
    my $table = ( $self->_has_select_priv(qw(dba_dependencies)) ) ? 'dba_dependencies' : 'all_dependencies';

    # ASSERTION: Names for tables, views, materialized views, functions,
    #   packages, and procedures are unique.

    # PROBLEM: The underlying table for materialized views does share
    # the same name as the materialized view-- thereby causing apparent
    # duplicates.

    # TODO: Problem Views/Materialized views that are dependent on another
    # view/materialized view will show both the other view/materialized
    # view AND the dependencies for the other view/materialized view.
    # (Essentially showing two levels of dependencies)

    my $str = qq{
SELECT DISTINCT d.owner,
        d.name,
        d.type,
        d.referenced_owner,
        d.referenced_name,
        coalesce ( mv.referenced_type, d.referenced_type ) AS referenced_type
    FROM sys.$table d
    LEFT OUTER JOIN sys.$table mv
        ON ( mv.referenced_owner = d.referenced_owner
            AND mv.referenced_name = d.referenced_name
            AND mv.referenced_type = 'MATERIALIZED VIEW' )
    WHERE d.owner = ? $table_filter
        AND d.referenced_owner NOT IN ( $not_in )
        AND d.referenced_type <> 'MATERIALIZED VIEW'
        AND ( d.owner || '.' || d.name <> d.referenced_owner || '.' || d.referenced_name )
};

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $name = $row->[1];
        my %v;
        $v{owner}             = $row->[0];
        $v{schema}            = $row->[0];
        $v{name}              = $row->[1];
        $v{type}              = $row->[2];
        $v{referenced_schema} = $row->[3];
        $v{referenced_name}   = $row->[4];
        $v{referenced_type}   = $row->[5];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_dependents {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving dependent object information ...");
    my %return;
    my $schema       = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'd.referenced_name', $filters );
    my $not_in       = $self->{sysuser_list};
    my $dep_table    = ( $self->_has_select_priv(qw(dba_dependencies)) ) ? 'dba_dependencies' : 'all_dependencies';
    my $trig_table   = ( $self->_has_select_priv(qw(dba_dependencies)) ) ? 'dba_triggers' : 'all_triggers';

    # NOTE: Listing dependencies of triggers on their attached table is
    # useless information. Triggers that are dependent on other tables
    # however ARE of interest.

    # PROBLEM: The underlying table for materialized views shares
    # the same name as the materialized view-- thereby causing apparent
    # duplicates. PACKAGE/PACKAGE BODY and TYPE/TYPE BODY behave similarly.

    # TODO: Problem Views/Materialized views that are dependent on another
    # view/materialized view will show both the other view/materialized
    # view AND the dependencies for the other view/materialized view.
    # (Essentially showing two levels of dependencies)

    my $str = qq{
SELECT DISTINCT d.referenced_owner,
        d.referenced_name,
        coalesce ( mv.referenced_type, d.referenced_type ) AS referenced_type,
        d.owner,
        d.name,
        CASE
            WHEN d.type = 'PACKAGE BODY'
            THEN 'PACKAGE'
            WHEN d.type = 'TYPE BODY'
            THEN 'TYPE'
            ELSE d.type
        END AS type
    FROM sys.$dep_table d
    LEFT OUTER JOIN sys.$trig_table t
        ON ( d.owner = t.table_owner
            AND d.name = t.table_name )
    LEFT OUTER JOIN sys.$dep_table mv
        ON ( mv.referenced_owner = d.referenced_owner
            AND mv.referenced_name = d.referenced_name
            AND mv.referenced_type = 'MATERIALIZED VIEW' )
    WHERE d.referenced_owner = ? $table_filter
        AND d.referenced_type <> 'MATERIALIZED VIEW'
        AND ( ( d.type <> 'TRIGGER'
                AND d.owner || '.' || d.name <> d.referenced_owner || '.' || d.referenced_name )
            OR ( d.type = 'TRIGGER'
                AND ( t.table_name <> d.name
                    OR d.referenced_owner <> d.owner ) ) )
};

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $name = $row->[1];
        my %v;
        $v{owner}             = $row->[0];
        $v{schema}            = $row->[0];
        $v{name}              = $row->[1];
        $v{type}              = $row->[2];
        $v{referenced_schema} = $row->[3];
        $v{referenced_name}   = $row->[4];
        $v{referenced_type}   = $row->[5];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_foreign_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving foreign key information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $str = qq{
SELECT owner AS table_schema,
        table_name,
        constraint_name,
        'ON DELETE ' || delete_rule AS constraint_rule,
        CASE status
            WHEN 'ENABLED' THEN 'Enabled'
            WHEN 'DISABLED' THEN 'Disabled'
            ELSE status
            END AS status
    FROM sys.all_constraints
    WHERE constraint_type = 'R'
        AND owner = ? $table_filter
};

    my $query = qq{
SELECT  col.column_name,
        r_col.owner AS r_table_schema,
        r_col.table_name AS r_table_name,
        r_col.column_name AS r_column_name,
        col.position
    FROM sys.all_constraints con
    INNER JOIN sys.all_cons_columns col
        ON ( con.owner = col.owner
            AND con.table_name = col.table_name
            AND con.constraint_name = col.constraint_name )
    INNER JOIN sys.all_cons_columns r_col
        ON ( con.r_owner = r_col.owner
            AND con.r_constraint_name = r_col.constraint_name )
    WHERE con.constraint_type = 'R'
        AND col.position = r_col.position
        AND con.owner = ?
        AND con.table_name = ?
        AND con.constraint_name = ?
    ORDER BY col.position
};

    my $sth = $self->_db_prepare($query);

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_schema = $row->[0];
        my $table_name   = $row->[1];
        my $cons_name    = $row->[2];
        $return{$table_name}{$cons_name}{table_schema}    = $table_schema;
        $return{$table_name}{$cons_name}{table_name}      = $table_name;
        $return{$table_name}{$cons_name}{constraint_name} = $cons_name;
        $return{$table_name}{$cons_name}{constraint_rule} = $row->[3];
        $return{$table_name}{$cons_name}{status}          = $row->[4];
        $return{$table_name}{$cons_name}{comments}        = '';

        $sth->execute( $table_schema, $table_name, $cons_name ) || $self->_log_fatal( $sth->errstr );
        foreach my $line ( @{ $sth->fetchall_arrayref } ) {
            push @{ $return{$table_name}{$cons_name}{column_names} },   $line->[0];
            push @{ $return{$table_name}{$cons_name}{r_column_names} }, $line->[3];
            $return{$table_name}{$cons_name}{r_table_schema} = $line->[1];
            $return{$table_name}{$cons_name}{r_table_name}   = $line->[2];
        }
    }
    return wantarray ? %return : \%return;
}

sub get_indexes {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving index information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'i.table_name', $filters );

    # ... With primary key indices filtered out and functional indexes added
    # TODO: actually filter on dba_constraints.constraint_type = 'P' for PKs

    my $str = qq{
SELECT  i.table_owner AS table_schema,
        i.table_name,
        i.index_name,
        CASE i.uniqueness
            WHEN 'UNIQUE' THEN 'Y'
            ELSE 'N'
            END AS is_unique,
        ic.column_name,
        ic.column_position,
        ic.descend,
        ie.column_expression
    FROM dba_indexes i
    INNER JOIN dba_ind_columns ic
        ON ( ic.index_owner = i.owner
            AND i.index_name = ic.index_name )
    LEFT OUTER JOIN dba_constraints c
        ON ( c.owner = i.owner
            AND c.table_name = i.table_name
            AND c.constraint_name = i.index_name )
    LEFT OUTER JOIN dba_ind_expressions ie
        ON ( ie.index_owner = ic.index_owner
            AND ie.index_name = ic.index_name
            AND ie.table_owner = ic.table_owner
            AND ie.table_name = ic.table_name
            AND ie.column_position = ic.column_position )
    WHERE i.owner = ? $table_filter
    ORDER BY i.table_name,
        i.index_name,
        ic.column_position
};

    $self->_set_longreadlen();
    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_schema = $row->[0];
        my $table_name   = $row->[1];
        my $index_name   = $row->[2];
        unless ( exists $return{$table_name}{$index_name}{table_schema} ) {
            $return{$table_name}{$index_name}{table_schema} = $table_schema;
            $return{$table_name}{$index_name}{is_unique}    = $row->[3];
            $return{$table_name}{$index_name}{comments}     = '';
        }
        my $column_name       = $row->[4];
        my $column_expression = $row->[7];
        if ($column_expression) {

            # Decending index columns are stored as function based indexes where the column name is quoted...
            $column_expression =~ s/^"(.*)"$/$1/;
            $column_name = $column_expression;
        }
        push @{ $return{$table_name}{$index_name}{column_names} }, $column_name;
        push @{ $return{$table_name}{$index_name}{decends} },      $row->[6];
        $self->_clear_longreadlen();
    }
    return wantarray ? %return : \%return;
}

sub get_partitions {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving partition information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'a.table_name', $filters );

    # TODO : sys.all_* or sys.dba_* tables?

    my $query = qq{
SELECT a.table_owner AS table_schema,
        a.table_name,
        a.partition_position,
        a.partition_name,
        a.high_value,
        a.tablespace_name,
        b.partitioning_type,
        c.name,
        c.column_name,
        c.column_position
    FROM sys.all_tab_partitions a
    INNER JOIN sys.all_part_tables b
        ON ( a.table_name = b.table_name )
    INNER JOIN sys.all_part_key_columns c
        ON ( a.table_name = c.name )
    WHERE b.partitioning_type IN ( 'RANGE', 'LIST' )
        AND a.table_owner = ? $table_filter
    ORDER BY a.table_name,
        a.partition_position,
        c.column_position
};

    $self->_set_longreadlen();
    my $sth = $self->_db_prepare($query);
    #    my @column_names = map { lc $_ } @{$sth->{NAME}};
    my @column_names = @{ $sth->{NAME_lc} };

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my %v                  = map { $column_names[$_] => $row->[$_] } ( 0 .. $#column_names );
        my $table_name         = $v{table_name};
        my $partition_position = $v{partition_position};
        my $partition_name     = $v{partition_name};
        my $high_value         = $v{high_value};

        if ( $high_value eq 'MAXVALUE' || $high_value eq 'DEFAULT' ) {
            $return{default}{$table_name} = $partition_name;
        }
        push @{ $return{parts}{$table_name}{$partition_position}{$partition_name} }, \%v;
    }
    $self->_clear_longreadlen();
    return wantarray ? %return : \%return;
}

sub get_primary_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving primary key information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $str = qq{
SELECT owner AS table_schema,
        table_name,
        constraint_name,
        CASE status
            WHEN 'ENABLED' THEN 'Enabled'
            WHEN 'DISABLED' THEN 'Disabled'
            ELSE status
            END AS status
    FROM sys.all_constraints
    WHERE constraint_type = 'P'
        AND owner = ? $table_filter
};

    my $query = qq{
SELECT column_name,
        position
    FROM sys.all_cons_columns
    WHERE owner = ?
        AND table_name = ?
        AND constraint_name = ?
    ORDER BY position
};

    my $sth = $self->_db_prepare($query);

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_schema    = $row->[0];
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        $return{$table_name}{table_schema}    = $table_schema;
        $return{$table_name}{table_name}      = $table_name;
        $return{$table_name}{constraint_name} = $constraint_name;
        $return{$table_name}{status}          = $row->[3];
        $return{$table_name}{comments}        = '';

        $sth->execute( $table_schema, $table_name, $constraint_name ) || $self->_log_fatal( $sth->errstr );
        foreach my $line ( @{ $sth->fetchall_arrayref } ) {
            push @{ $return{$table_name}{column_names} },     $line->[0];
            push @{ $return{$table_name}{column_positions} }, $line->[1];
        }
    }
    return wantarray ? %return : \%return;
}

sub get_schemas {
    my ( $self, @schemas ) = @_;
    $self->{logger}->log_info("Retrieving schema information ...");
    my %return;

    my $schema =
        (@schemas)
        ? "\n    AND owner IN (" . $self->_array_to_str(@schemas) . ")"
        : '';

    my $valid = ( $self->{export_invalid} ) ? '' : "\n    AND status = 'VALID'";
    my $not_in = $self->{sysuser_list};

    my $tab_table = ( $self->_has_select_priv(qw(dba_tables)) )  ? 'dba_tables'  : 'all_tables';
    my $obj_table = ( $self->_has_select_priv(qw(dba_objects)) ) ? 'dba_objects' : 'all_objects';

    my $str = qq{
SELECT DISTINCT owner
    FROM sys.$tab_table
    WHERE owner NOT IN ( $not_in ) $schema
UNION
SELECT owner
    FROM sys.$obj_table
    WHERE object_type IN ( 'TABLE', 'VIEW', 'MATERIALIZED VIEW' )
        AND owner NOT IN ( $not_in ) $valid $schema
};

    foreach my $row ( $self->_db_query($str) ) {
        my $name = $row->[0];
        $return{$name}{catalog_name} = '';
        $return{$name}{schema_name}  = $name;
        $return{$name}{schema_owner} = $name;
        $return{$name}{comments}     = '';
    }
    return wantarray ? %return : \%return;
}

sub get_sequences {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving sequence information ...");
    my %return;
    my $schema = $self->{schema};

    my $str = qq{
SELECT sequence_owner,
        sequence_name,
        min_value,
        max_value,
        increment_by,
        last_number,
        cache_size,
        cycle_flag
    FROM sys.all_sequences
    WHERE sequence_owner = ?
};

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $name = $row->[1];
        $return{$name}{sequence_schema} = $row->[0];
        $return{$name}{sequence_owner}  = $row->[0];
        $return{$name}{sequence_name}   = $row->[1];
        $return{$name}{min_value}       = $row->[2];
        $return{$name}{max_value}       = $row->[3];
        $return{$name}{increment_by}    = $row->[4];
        $return{$name}{last_number}     = $row->[5];
        $return{$name}{cache_size}      = $row->[6];
        $return{$name}{cycle_flag}      = $row->[7];
    }
    return wantarray ? %return : \%return;
}

sub get_table_columns {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_debug("Extracting table column information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'col.table_name', $filters );

    my $query = qq{
SELECT col.owner AS table_schema,
        col.table_name,
        col.column_name,
        col.column_id AS ordinal_position,
        col.data_type,
        col.data_length,
        col.data_precision,
        col.data_scale,
        col.char_length,
        col.nullable AS is_nullable,
        col.data_default,
        cmt.comments
    FROM sys.all_tab_columns col
    LEFT OUTER JOIN sys.all_col_comments cmt
         ON ( col.owner = cmt.owner
            AND col.table_name = cmt.table_name
            AND col.column_name = cmt.column_name )
    WHERE col.owner = ? $table_filter
    ORDER BY col.column_id
};

    $self->_set_longreadlen();
    my $sth = $self->_db_prepare($query);
    #    my @column_names = map { lc $_ } @{$sth->{NAME}};
    my @column_names = @{ $sth->{NAME_lc} };

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $table_name  = $row->[1];
        my $column_name = $row->[2];
        foreach my $idx ( 0 .. $#column_names ) {
            $return{$table_name}{columns}{$column_name}{ $column_names[$idx] } = $row->[$idx];
        }
        push @{ $return{$table_name}{column_names} }, $row->[2];

        my @ary =
            map { $return{$table_name}{columns}{$column_name}{$_} }
            (qw(data_type data_length data_precision data_scale char_length));
        push @{ $return{$table_name}{data_types} }, $self->_format_sql_type(@ary);
    }
    $self->_clear_longreadlen();

    return wantarray ? %return : \%return;
}

sub get_table_privs {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table privilege information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    # ASSERTION: Names for tables, views, materialized, functions,
    #   packages, and procedures are unique.
    my $str = '';
    if ( $self->_has_select_priv('dba_tab_privs') ) {
        $str = qq{
SELECT owner AS table_schema,
        table_name,
        grantee,
        privilege
    FROM sys.dba_tab_privs
    WHERE owner = ? $table_filter
};
    }
    else {
        $str = qq{
SELECT table_schema,
        table_name,
        grantee,
        privilege
    FROM sys.all_tab_privs
    WHERE table_schema = ? $table_filter
};
    }

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $name = $row->[1];
        my %v;
        $v{schema}    = $row->[0];
        $v{name}      = $row->[1];
        $v{grantee}   = $row->[2];
        $v{privilege} = $row->[3];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_tables {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table information ...");

    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $query = qq{
SELECT '' AS table_catalog,
        owner AS table_schema,
        owner AS table_owner,
        table_type,
        table_name,
        tablespace_name,
        row_count,
        column_count,
        comments,
        '' AS query
    FROM (
        SELECT  tab.owner,
                tab.table_name,
                CASE
                    WHEN mv.table_name IS NOT NULL
                    THEN 'MATERIALIZED VIEW'
                    ELSE 'TABLE'
                    END AS table_type,
                tab.tablespace_name,
                to_char(tab.num_rows) AS row_count,
                col.num_cols AS column_count,
                cmt.comments
            FROM sys.all_tables tab
            LEFT OUTER JOIN sys.all_tab_comments cmt
                ON ( tab.owner = cmt.owner
                    AND tab.table_name = cmt.table_name )
            LEFT OUTER JOIN sys.all_snapshots mv
                ON ( tab.owner = mv.owner
                    AND tab.table_name = mv.table_name )
            INNER JOIN (
                    SELECT  owner,
                            table_name,
                            count ( table_name ) AS num_cols
                        FROM sys.all_tab_columns
                        GROUP BY owner, table_name
                    ) col
                ON ( tab.owner = col.owner
                    AND tab.table_name = col.table_name )
        UNION
        SELECT  v.owner,
                v.view_name,
                'VIEW',
                '',
                '',
                num_cols,
                cmt.comments
            FROM sys.all_views v
            LEFT OUTER JOIN sys.all_tab_comments cmt
                ON ( v.owner = cmt.owner
                    AND v.view_name = cmt.table_name )
            INNER JOIN (
                    SELECT  owner,
                            table_name,
                            count ( table_name ) AS num_cols
                        FROM sys.all_tab_columns
                        GROUP BY owner, table_name
                    ) col
                ON ( v.owner = col.owner
                    AND v.view_name = col.table_name )
        ) a
    WHERE owner = ? $table_filter
    ORDER BY 3,4
};

    my $sth = $self->_db_prepare($query);
    #    my @column_names = map { lc $_ } @{$sth->{NAME}};
    my @column_names = @{ $sth->{NAME_lc} };

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $table_name = $row->[4];
        foreach my $idx ( 0 .. $#column_names ) {
            $return{$table_name}{ $column_names[$idx] } = $row->[$idx];
        }
    }

    ####################################################################
    my $view_filter = $self->_get_table_filter( 'view_name', $filters );
    my $v_query = qq{
SELECT view_name,
        text
    FROM sys.all_views
    WHERE owner = ? $view_filter
};

    $self->_set_longreadlen();
    my $v_sth = $self->_db_prepare($v_query);
    $v_sth->execute($schema) || $self->_log_fatal( $v_sth->errstr );
    foreach my $row ( @{ $v_sth->fetchall_arrayref } ) {
        my $table_name = $row->[0];
        if ( exists $return{$table_name} ) {
            $return{$table_name}{query} = $row->[1];
        }
    }

    ####################################################################
    # my $mview_filter = Should match the table filter
    my $mv_query = qq{
SELECT table_name,
        query
    FROM sys.all_snapshots
    WHERE owner = ? $table_filter
};

    my $mv_sth = $self->_db_prepare($mv_query);
    $mv_sth->execute($schema) || $self->_log_fatal( $mv_sth->errstr );
    foreach my $row ( @{ $mv_sth->fetchall_arrayref } ) {
        my $table_name = $row->[0];
        if ( exists $return{$table_name} ) {
            $return{$table_name}{query} = $row->[1];
        }
    }
    $self->_clear_longreadlen;
    return wantarray ? %return : \%return;
}

sub get_tablespaces {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving tablespace information ...");
    my %return;
    my $schema = $self->{schema};

    if ( $self->_has_select_priv(qw(dba_segments dba_objects dba_data_files)) ) {
        my $str = qq{
SELECT a.tablespace_name,
        c.file_name,
        a.segment_type,
        a.segment_name,
        '' AS comments
    FROM dba_segments a
    INNER JOIN dba_objects b
        ON ( a.segment_name = b.object_name
            AND a.segment_type = b.object_type
            AND a.owner = b.owner )
    INNER JOIN dba_data_files c
        ON ( a.tablespace_name = c.tablespace_name )
    WHERE a.segment_type IN ( 'INDEX', 'TABLE' )
        AND a.owner = ?
        AND a.tablespace_name NOT IN  ('SYSTEM', 'SYSAUX', 'TOOLS' )
};

        foreach my $row ( $self->_db_query( $str, $schema ) ) {
            my $name = $row->[0];
            $return{$name}{tablespace_name} = $row->[0];
            $return{$name}{file_name}       = $row->[1];
            $return{$name}{segment_type}    = $row->[2];
            $return{$name}{segment_name}    = $row->[3];
            $return{$name}{comments}        = $row->[4];
        }
    }
    else {
        $self->{logger}->log_error("You do not have rights to query the tablespace information!");
    }
    return wantarray ? %return : \%return;
}

sub get_triggers {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving trigger information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $str = qq{
SELECT owner AS table_schema,
        trigger_name,
        trigger_type,
        triggering_event,
        table_name,
        trigger_body,
        when_clause,
        description
    FROM sys.all_triggers
    WHERE owner = ? $table_filter
        AND status = 'ENABLED'
};

    $self->_set_longreadlen();
    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $trigger_name = $row->[1];
        my $table_name   = $row->[4];
        $return{$table_name}{$trigger_name}{table_schema}     = $row->[0];
        $return{$table_name}{$trigger_name}{trigger_name}     = $trigger_name;
        $return{$table_name}{$trigger_name}{trigger_type}     = $row->[2];
        $return{$table_name}{$trigger_name}{triggering_event} = $row->[3];
        $return{$table_name}{$trigger_name}{table_name}       = $table_name;
        $return{$table_name}{$trigger_name}{trigger_body}     = $row->[5];
        $return{$table_name}{$trigger_name}{when_clause}      = $row->[6];
        $return{$table_name}{$trigger_name}{description}      = $row->[7];
    }
    $self->_clear_longreadlen();
    return wantarray ? %return : \%return;
}

sub get_unique_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving unique constraint information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'table_name', $filters );

    my $str = qq{
SELECT owner AS table_schema,
        table_name,
        constraint_name,
        CASE status
            WHEN 'ENABLED' THEN 'Enabled'
            WHEN 'DISABLED' THEN 'Disabled'
            ELSE status
            END AS status
    FROM sys.all_constraints
    WHERE constraint_type = 'U'
        AND owner = ? $table_filter
};

    my $query = qq{
SELECT column_name,
        position
    FROM sys.all_cons_columns
    WHERE owner = ?
        AND table_name = ?
        AND constraint_name = ?
    ORDER BY position
};

    my $sth = $self->_db_prepare($query);

    foreach my $row ( $self->_db_query( $str, $schema ) ) {
        my $table_schema = $row->[0];
        my $table_name   = $row->[1];
        my $cons_name    = $row->[2];
        $return{$table_name}{$cons_name}{table_schema}    = $table_schema;
        $return{$table_name}{$cons_name}{table_name}      = $table_name;
        $return{$table_name}{$cons_name}{constraint_name} = $cons_name;
        $return{$table_name}{$cons_name}{status}          = $row->[3];
        $return{$table_name}{$cons_name}{comments}        = '';

        $sth->execute( $table_schema, $table_name, $cons_name ) || $self->_log_fatal( $sth->errstr );
        foreach my $line ( @{ $sth->fetchall_arrayref } ) {
            push @{ $return{$table_name}{$cons_name}{column_names} }, $line->[0];
        }
    }
    return wantarray ? %return : \%return;
}

sub _get_object_source {
    my ( $self, $object_type, $object_name ) = @_;
    $self->{logger}->log_info( "Retrieving " . lc $object_type . " information ..." );
    my %return;
    my $schema = $self->{schema};

    $object_name =
        ($object_name)
        ? "\n    AND object_name = '$object_name'"
        : '';

    my $owner =
        ( $object_type eq 'TYPE' || $object_type eq 'TYPE BODY' )
        ? "owner IN ( '$schema', 'SYSTEM' )"
        : "owner = '$schema'";
    my $valid = ( $self->{export_invalid} ) ? '' : "\n    AND status = 'VALID'";

    my $str = qq{
SELECT owner AS object_schema,
        object_name
    FROM all_objects
    WHERE $owner
        AND object_type = '$object_type' $valid
};

    my $query = "SELECT text FROM all_source WHERE owner = ? AND name = ? AND type = ? ORDER BY line";
    my $sth   = $self->_db_prepare($query);

    $self->_set_longreadlen();
    foreach my $row ( $self->_db_query($str) ) {
        my $object_schema = $row->[1];
        my $object_name   = $row->[1];
        $return{$object_name}{object_schema} = $object_schema;
        $return{$object_name}{object_owner}  = $object_schema;
        $return{$object_name}{object_name}   = $object_name;
        $return{$object_name}{comment}       = '';
        my @source;
        $sth->execute( $object_schema, $object_name, $object_type );

        if ( $sth->errstr ) {
            $self->_log_fatal( $sth->errstr );
        }

        foreach my $line ( @{ $sth->fetchall_arrayref } ) {
            push @source, $line->[0];
        }
        $return{$object_name}{source} .= join( '', @source );

        if ( $object_type eq 'TYPE' || $object_type eq 'PACKAGE' ) {
            my @body_source;
            $sth->execute( $object_schema, $object_name, "$object_type BODY" );
            if ( $sth->errstr ) {
                $self->_log_fatal( $sth->errstr );
            }

            foreach my $line ( @{ $sth->fetchall_arrayref } ) {
                push @body_source, $line->[0];
            }
            $return{$object_name}{body_source} .= join( '', @body_source );
        }
    }

    $self->_clear_longreadlen();
    return wantarray ? %return : \%return;
}

sub _format_sql_type {
    my ( $self, $data_type, $data_length, $data_precision, $data_scale, $char_length ) = @_;

    my $formatted_type = $data_type;

    if ( uc $data_type eq 'NUMBER' ) {
        if ( $data_precision && $data_scale ) {
            $formatted_type .= "($data_precision,$data_scale)";
        }
        elsif ($data_precision) {
            $formatted_type .= "($data_precision)";
        }
        elsif ($data_length) {
            $formatted_type .= "($data_length)";
        }
    }
    elsif ( uc $data_type eq 'CHAR' || uc $data_type eq 'NCHAR' ) {
        $formatted_type .= "($char_length)" if ($char_length);
    }
    elsif ( uc $data_type eq 'VARCHAR2' || uc $data_type eq 'NVARCHAR2' ) {
        $formatted_type .= "($char_length)";
    }
    elsif ( uc $data_type eq 'FLOAT' ) {
        $formatted_type .= "($data_precision)" if ($data_precision);
    }
    elsif ( uc $data_type eq 'RAW' ) {
        $formatted_type .= "($data_length)";
    }
    elsif ( uc $data_type eq 'UROWID' ) {
        $formatted_type .= "($data_length)" if ($data_length);
    }

    return $formatted_type;
}

1;
__END__
