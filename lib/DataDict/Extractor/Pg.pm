package DataDict::Extractor::Pg;
use base 'DataDict::Extractor';

use strict;
use warnings;
use Data::Dumper;

# TODO: prepare and extract user query as table

sub _post_init {
    my ($self) = @_;
}

sub _has_priv {
    my ( $self, @needs ) = @_;
    foreach my $needed (@needs) {
        return 0 unless ( exists $self->{select_privs}{ lc $needed } );
    }
    return 1;
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

        # TODO :

        # $rv = $dbh->quote($value, $data_type); ???
        # $rv = $sth->bind_param($param_num, $bind_value,
        #                         { pg_type => PG_BYTEA });

        if ( $data_type =~ /^(date|time)/i ) {
            push @ary, $column_name . '::text AS ' . $column_name;
        }
        elsif ( $data_type =~ /^xml/i ) {    # XMLSERIALIZE ( { DOCUMENT | CONTENT } value AS type )
            push @ary, qq{XMLSERIALIZE($column_name AS text) AS $column_name};
        }
        elsif ( $data_type =~ /(bytea)$/i ) {
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
        $self->_set_longreadlen( 1023 * 1024 );
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
    $self->{logger}->log_debug("Finished extracting data...");
    $sth->finish() if ($sth);
}

sub get_objects {
    my ( $self, $object_type, @args ) = @_;

    $object_type ||= 'UNKNOWN';

    if ( uc $object_type eq 'CHECK CONSTRAINT' )  { return $self->get_check_constraints(@args); }
    if ( uc $object_type eq 'CHILD KEY' )         { return $self->get_child_keys(@args); }
    if ( uc $object_type eq 'COLUMN' )            { return $self->get_table_columns(@args); }
    if ( uc $object_type eq 'DEPENDENCY' )        { return $self->get_dependencies(@args); }
    if ( uc $object_type eq 'DEPENDENT' )         { return $self->get_dependents(@args); }
    if ( uc $object_type eq 'DOMAIN' )            { return $self->get_domains(@args); }
    if ( uc $object_type eq 'FOREIGN KEY' )       { return $self->get_foreign_keys(@args); }
    if ( uc $object_type eq 'INDEX' )             { return $self->get_indexes(@args); }
    if ( uc $object_type eq 'PRIMARY KEY' )       { return $self->get_primary_keys(@args); }
    if ( uc $object_type eq 'SCHEMA' )            { return $self->get_schemas(@args); }
    if ( uc $object_type eq 'TABLE' )             { return $self->get_tables(@args); }
    if ( uc $object_type eq 'TYPE' )              { return $self->get_types(@args); }
    if ( uc $object_type eq 'UNIQUE CONSTRAINT' ) { return $self->get_unique_constraints(@args); }

    $self->{logger}->log_error("No extraction routine available for $object_type objects...");

    # NOTE: The Oracle also supports COLUMN PRIV, FUNCTION, PACKAGE, PARTITION,
    #   PROCEDURE, SEQUENCE, TABLE PRIV, TABLESPACE, and TRIGGER. Do we want/need
    #   to support any of these?

    return undef;
}

sub get_check_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Extracting check_constraint information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        r.relname AS table_name,
        c.conname AS constraint_name,
        c.consrc AS search_condition,
        d.description AS comments
    FROM pg_class r
    INNER JOIN pg_namespace nr
        ON ( nr.oid = r.relnamespace )
    INNER JOIN pg_constraint c
        ON ( c.conrelid = r.oid )
    INNER JOIN pg_namespace nc
        ON ( nc.oid = c.connamespace )
    LEFT OUTER JOIN pg_description d
        ON ( d.objoid = c.oid )
    WHERE r.relkind = 'r'
        AND c.contype = 'c'
        AND nr.nspname = ? $table_filter
    ORDER BY r.relname,
        c.conname
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        $return{$table_name}{$constraint_name}{table_schema}     = $row->[0];
        $return{$table_name}{$constraint_name}{table_name}       = $table_name;
        $return{$table_name}{$constraint_name}{constraint_name}  = $constraint_name;
        $return{$table_name}{$constraint_name}{search_condition} = $row->[3];
        $return{$table_name}{$constraint_name}{status}           = 'Enabled';
        $return{$table_name}{$constraint_name}{comments}         = $row->[4];
    }

    return wantarray ? %return : \%return;
}

sub get_child_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving child key information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        r.relname AS table_name,
        c.conname AS constraint_name,
        split_part ( split_part ( pg_catalog.pg_get_constraintdef ( c.oid, true ), '(', 3 ), ')', 1 ) AS column_names,
        nr2.nspname AS r_table_schema,
        r2.relname AS r_table_name,
        split_part ( split_part ( pg_catalog.pg_get_constraintdef ( c.oid, true ), '(', 2 ), ')', 1 ) AS r_column_names,
        CASE c.confmatchtype
            WHEN 'f' THEN 'MATCH FULL '
            WHEN 'p' THEN 'MATCH PARTIAL '
            WHEN 'u' THEN 'MATCH NONE '
            ELSE ''
            END
        || CASE c.confupdtype
            WHEN 'c' THEN 'ON UPDATE CASCADE '
            WHEN 'n' THEN 'ON UPDATE SET NULL '
            WHEN 'd' THEN 'ON UPDATE SET DEFAULT '
            WHEN 'r' THEN 'ON UPDATE RESTRICT '
            WHEN 'a' THEN 'ON UPDATE NO ACTION '
            ELSE ''
            END
        || CASE c.confdeltype
            WHEN 'c' THEN 'ON DELETE CASCADE'
            WHEN 'n' THEN 'ON DELETE SET NULL'
            WHEN 'd' THEN 'ON DELETE SET DEFAULT'
            WHEN 'r' THEN 'ON DELETE RESTRICT'
            WHEN 'a' THEN 'ON DELETE NO ACTION'
            ELSE ''
            END AS constraint_rule,
        d.description AS comments
    FROM pg_catalog.pg_constraint c
    INNER JOIN pg_catalog.pg_class r
        ON ( r.oid = c.confrelid )
    INNER JOIN pg_catalog.pg_class r2
        ON ( r2.oid = c.conrelid )
    INNER JOIN pg_namespace nr
        ON ( nr.oid = r.relnamespace )
    INNER JOIN pg_namespace nr2
        ON ( nr2.oid = r2.relnamespace )
    LEFT OUTER JOIN pg_catalog.pg_description d
        ON ( d.objoid = c.oid )
    WHERE c.contype = 'f'
        AND nr.nspname = ? $table_filter
    ORDER BY nr2.nspname,
        r2.relname

};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_schema    = $row->[0];
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        my $r_table_schema  = $row->[4];
        $return{$table_name}{$r_table_schema}{$constraint_name}{table_schema}    = $table_schema;
        $return{$table_name}{$r_table_schema}{$constraint_name}{table_name}      = $table_name;
        $return{$table_name}{$r_table_schema}{$constraint_name}{constraint_name} = $constraint_name;
        @{ $return{$table_name}{$r_table_schema}{$constraint_name}{column_names} } = split ',', $row->[3];
        $return{$table_name}{$r_table_schema}{$constraint_name}{r_table_schema} = $row->[4];
        $return{$table_name}{$r_table_schema}{$constraint_name}{r_table_name}   = $row->[5];
        @{ $return{$table_name}{$r_table_schema}{$constraint_name}{r_column_names} } = split ',', $row->[6];
        $return{$table_name}{$r_table_schema}{$constraint_name}{constraint_rule} = $row->[7];
        $return{$table_name}{$r_table_schema}{$constraint_name}{status}          = 'Enabled';
        $return{$table_name}{$r_table_schema}{$constraint_name}{comments}        = $row->[8];
    }
    return wantarray ? %return : \%return;
}

sub get_db_encoding {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database encoding information...");
    my ($row) = $self->_db_fetch('SELECT pg_catalog.getdatabaseencoding ()');
    return $row->[0];
}

sub get_db_comment {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database comment information...");

    my $query = q{
SELECT pg_catalog.shobj_description ( d.oid, 'pg_database' ) AS COMMENT
    FROM pg_catalog.pg_database d
    WHERE d.datname = pg_catalog.current_database ()
};

    my ($row) = $self->_db_fetch($query);
    return $row->[0];
}

sub get_db_version {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database version information...");
    my ($row) = $self->_db_fetch('SELECT pg_catalog.version ()');
    return $row->[0];
}

sub get_dependencies {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving object dependency information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'c.relname', $filters );

    my $query = qq{
WITH dependents AS (
    SELECT ev_class,
            split_part ( regexp_split_to_table ( ev_action, ':relid ' ), ' ', 1 ) AS dependent_oid
        FROM pg_rewrite
),
dep_map AS (
    SELECT ev_class AS parent_oid,
            dependent_oid::oid AS child_oid
        FROM dependents
        WHERE dependent_oid NOT LIKE '%QUERY'
            AND ev_class <> dependent_oid::oid
)
SELECT DISTINCT pg_catalog.pg_get_userbyid ( c.relowner ) AS table_owner,
        n.nspname AS table_schema,
        c.relname AS table_name,
        CASE c.relkind
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            END AS table_type,
        n2.nspname AS referenced_schema,
        c2.relname AS referenced_name,
        CASE c2.relkind
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'r' THEN 'TABLE'
            WHEN 't' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            END AS referenced_type
    FROM pg_catalog.pg_class c
    LEFT OUTER JOIN pg_catalog.pg_namespace n
        ON ( n.oid = c.relnamespace )
    INNER JOIN dep_map d
        ON ( c.oid = d.parent_oid )
    INNER JOIN pg_catalog.pg_class c2
        ON ( c2.oid = d.child_oid )
    LEFT OUTER JOIN pg_catalog.pg_namespace n2
        ON ( n2.oid = c2.relnamespace )
    WHERE c.relkind IN ( 'v', 'r', 'f', 'm' )
        AND n.nspname = ? $table_filter
};
    # from pg_class.h
    # #define           RELKIND_INDEX           'i'           /* secondary index */
    # #define           RELKIND_RELATION        'r'           /* ordinary cataloged heap */
    # #define           RELKIND_SEQUENCE        'S'           /* SEQUENCE relation */
    # #define           RELKIND_UNCATALOGED     'u'           /* temporary heap */
    # #define           RELKIND_TOASTVALUE      't'           /* moved off huge values */
    # #define           RELKIND_VIEW            'v'           /* view */
    # #define           RELKIND_COMPOSITE_TYPE  'c'           /* composite type */

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $name = $row->[2];
        my %v;
        $v{owner}             = $row->[0];
        $v{schema}            = $row->[1];
        $v{name}              = $row->[2];
        $v{type}              = $row->[3];
        $v{referenced_schema} = $row->[4];
        $v{referenced_name}   = $row->[5];
        $v{referenced_type}   = $row->[6];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_dependents {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving dependent object information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'c2.relname', $filters );

    # Looking for a the following:
    # owner, name, type, referenced_owner, referenced_name, referenced_type

    # TODO: determine that which depends on a foreign table

    my $query = qq{
WITH dependents AS (
    SELECT ev_class,
            split_part ( regexp_split_to_table ( ev_action, ':relid ' ), ' ', 1 ) AS dependent_oid
        FROM pg_rewrite
),
dep_map AS (
    SELECT ev_class AS parent_oid,
            dependent_oid::oid AS child_oid
        FROM dependents
        WHERE dependent_oid NOT LIKE '%QUERY'
            AND ev_class <> dependent_oid::oid
)
SELECT DISTINCT pg_catalog.pg_get_userbyid ( c.relowner ) AS table_owner,
        n.nspname AS table_schema,
        c.relname AS table_name,
        CASE c.relkind
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            END AS table_type,
        n2.nspname AS referenced_schema,
        c2.relname AS referenced_name,
        CASE c2.relkind
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'r' THEN 'TABLE'
            WHEN 't' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            END AS referenced_type
    FROM pg_catalog.pg_class c
    LEFT OUTER JOIN pg_catalog.pg_namespace n
        ON ( n.oid = c.relnamespace )
    INNER JOIN dep_map d
        ON ( c.oid = d.child_oid )
    INNER JOIN pg_catalog.pg_class c2
        ON ( c2.oid = d.parent_oid )
    LEFT OUTER JOIN pg_catalog.pg_namespace n2
        ON ( n2.oid = c2.relnamespace )
    WHERE c.relkind IN ( 'v', 'r', 'f', 'm' )
        AND n.nspname = ? $table_filter
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $name = $row->[2];
        my %v;
        $v{owner}             = $row->[0];
        $v{schema}            = $row->[1];
        $v{name}              = $row->[2];
        $v{type}              = $row->[3];
        $v{referenced_schema} = $row->[4];
        $v{referenced_name}   = $row->[5];
        $v{referenced_type}   = $row->[6];

        push @{ $return{$name} }, \%v;
    }
    return wantarray ? %return : \%return;
}

sub get_domains {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving domain information...");
    my %return;
    my $schema = $self->{schema};

    my $query = q{
SELECT n.nspname AS domain_schema,
        t.typname AS domain_name,
        pg_catalog.pg_get_userbyid ( t.typowner ) AS domain_owner,
        pg_catalog.format_type ( t.typbasetype, t.typtypmod ) AS data_type,
        CASE
            WHEN t.typnotnull THEN 'N'
            ELSE 'Y'
            END AS is_nullable,
        t.typdefault AS domain_default,
        pg_catalog.array_to_string ( array (
                SELECT pg_catalog.pg_get_constraintdef ( r.oid, true )
                    FROM pg_catalog.pg_constraint r
                    WHERE t.oid = r.contypid
            ),
            ' ')
        AS check_constraint,
        pg_catalog.obj_description ( t.oid, 'pg_type' ) AS comments
    FROM pg_catalog.pg_type t
    LEFT OUTER JOIN pg_catalog.pg_namespace n
        ON n.oid = t.typnamespace
    WHERE t.typtype = 'd'
        AND pg_catalog.pg_type_is_visible ( t.oid )
        AND n.nspname = ?
    ORDER BY t.typname
};

    my $sth = $self->_db_prepare($query);
    # Because `my @column_names = @{$sth->{NAME_lc}};` isn't working
    my @column_names =
        (qw( domain_schema domain_name domain_owner data_type is_nullable domain_default check_constraint comments ));

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $domain_name = $row->[1];
        $return{$domain_name}{ $column_names[$_] } = $row->[$_] for ( 0 .. $#column_names );
    }
    return wantarray ? %return : \%return;
}

sub get_foreign_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving foreign key information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        r.relname AS table_name,
        c.conname AS constraint_name,
        split_part ( split_part ( pg_get_constraintdef ( c.oid ), '(', 2 ), ')', 1 ) AS column_names,
        nr2.nspname AS r_table_schema,
        r2.relname AS r_table_name,
        split_part ( split_part ( pg_get_constraintdef ( c.oid ), '(', 3 ), ')', 1 ) AS r_column_names,
        CASE c.confmatchtype
            WHEN 'f' THEN 'MATCH FULL '
            WHEN 'p' THEN 'MATCH PARTIAL '
            WHEN 'u' THEN 'MATCH NONE '
            ELSE ''
            END
        || CASE c.confupdtype
            WHEN 'c' THEN 'ON UPDATE CASCADE '
            WHEN 'n' THEN 'ON UPDATE SET NULL '
            WHEN 'd' THEN 'ON UPDATE SET DEFAULT '
            WHEN 'r' THEN 'ON UPDATE RESTRICT '
            WHEN 'a' THEN 'ON UPDATE NO ACTION '
            ELSE ''
            END
        || CASE c.confdeltype
            WHEN 'c' THEN 'ON DELETE CASCADE'
            WHEN 'n' THEN 'ON DELETE SET NULL'
            WHEN 'd' THEN 'ON DELETE SET DEFAULT'
            WHEN 'r' THEN 'ON DELETE RESTRICT'
            WHEN 'a' THEN 'ON DELETE NO ACTION'
            ELSE ''
            END AS constraint_rule,
        d.description AS comments
    FROM pg_class r
    INNER JOIN pg_namespace nr
        ON ( nr.oid = r.relnamespace )
    INNER JOIN pg_constraint c
        ON ( c.conrelid = r.oid )
    LEFT OUTER JOIN pg_description d
        ON ( d.objoid = c.oid )
    INNER JOIN pg_catalog.pg_class r2
        ON ( r2.oid = c.confrelid )
    INNER JOIN pg_namespace nr2
        ON ( nr2.oid = r2.relnamespace )
    WHERE r.relkind = 'r'
        AND c.contype = 'f'
        AND nr.nspname = ? $table_filter
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        $return{$table_name}{$constraint_name}{table_schema}    = $row->[0];
        $return{$table_name}{$constraint_name}{table_name}      = $table_name;
        $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
        @{ $return{$table_name}{$constraint_name}{column_names} } = split ',', $row->[3];
        $return{$table_name}{$constraint_name}{r_table_schema} = $row->[4];
        $return{$table_name}{$constraint_name}{r_table_name}   = $row->[5];
        @{ $return{$table_name}{$constraint_name}{r_column_names} } = split ',', $row->[6];
        $return{$table_name}{$constraint_name}{constraint_rule} = $row->[7];
        $return{$table_name}{$constraint_name}{status}          = 'Enabled';
        $return{$table_name}{$constraint_name}{comments}        = $row->[8];
    }
    return wantarray ? %return : \%return;
}

sub get_functions {

=pod


SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
 CASE
  WHEN p.proisagg THEN 'agg'
  WHEN p.proiswindow THEN 'window'
  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
  ELSE 'normal'
END as "Type",
 CASE
  WHEN p.provolatile = 'i' THEN 'immutable'
  WHEN p.provolatile = 's' THEN 'stable'
  WHEN p.provolatile = 'v' THEN 'volatile'
END as "Volatility",
  pg_catalog.pg_get_userbyid(p.proowner) as "Owner",
  l.lanname as "Language",
  p.prosrc as "Source code",
  pg_catalog.obj_description(p.oid, 'pg_proc') as "Description"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE p.proname ~ '^(test_func)$'
  AND n.nspname ~ '^(rapids)$'




=cut

}

sub get_indexes {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving index information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'c.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        c.relname AS table_name,
        c2.relname AS index_name,
        split_part ( split_part ( pg_catalog.pg_get_indexdef ( i.indexrelid, 0, true ), '(', 2 ), ')', 1 ) AS column_names,
        CASE
            WHEN i.indisunique THEN 'Y'
            ELSE 'N'
            END AS is_unique,
        d.description AS comments
    FROM pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_index i
        ON ( c.oid = i.indrelid )
    INNER JOIN pg_catalog.pg_class c2
        ON ( i.indexrelid = c2.oid )
    LEFT OUTER JOIN pg_catalog.pg_description d
        ON ( d.objoid = i.indexrelid )
    INNER JOIN pg_namespace nr
        ON ( nr.oid = c.relnamespace )
    WHERE nr.nspname = ? $table_filter
    ORDER BY nr.nspname,
        c.relname
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_name = $row->[1];
        my $index_name = $row->[2];
        $return{$table_name}{$index_name}{table_schema} = $row->[0];
        $return{$table_name}{$index_name}{table_name}   = $table_name;
        $return{$table_name}{$index_name}{index_name}   = $index_name;
        @{ $return{$table_name}{$index_name}{column_names} } = split ',', $row->[3];
        $return{$table_name}{$index_name}{is_unique} = $row->[4];
        $return{$table_name}{$index_name}{comments}  = $row->[5];
        # TODO : asc vs. desc index?
        @{ $return{$table_name}{$index_name}{decends} } = map { '' } split ',', $row->[3];
    }
    return wantarray ? %return : \%return;
}

sub get_primary_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving primary key information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        r.relname AS table_name,
        c.conname AS constraint_name,
        split_part ( split_part ( pg_get_constraintdef ( c.oid ), '(', 2 ), ')', 1 ) AS column_names,
        d.description AS comments
    FROM pg_class r
    INNER JOIN pg_namespace nr
        ON ( nr.oid = r.relnamespace )
    INNER JOIN pg_constraint c
        ON ( c.conrelid = r.oid )
    INNER JOIN pg_namespace nc
        ON ( nc.oid = c.connamespace )
    LEFT OUTER JOIN pg_description d
        ON ( d.objoid = c.oid )
    WHERE r.relkind = 'r'
        AND c.contype = 'p'
        AND c.contype <> 'f'
        AND nr.nspname = ? $table_filter
    ORDER BY c.conname
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_name = $row->[1];
        $return{$table_name}{table_schema}    = $row->[0];
        $return{$table_name}{table_name}      = $table_name;
        $return{$table_name}{constraint_name} = $row->[2];
        @{ $return{$table_name}{column_names} } = split ',', $row->[3];
        $return{$table_name}{status}   = 'Enabled';
        $return{$table_name}{comments} = $row->[4];
    }
    return wantarray ? %return : \%return;
}

sub get_schema_comment {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving schema comment information...");
    my %return;
    my $schema = $self->{schema};

    my $query = qq{
SELECT pg_catalog.obj_description ( n.oid, 'pg_namespace' ) AS comments
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname NOT LIKE 'pg_%'
        AND n.nspname = ?
};

    my ($row) = $self->_db_fetch($query);
    return $row->[0];
}

sub get_schemas {
    my ( $self, @schemas ) = @_;
    $self->{logger}->log_info("Retrieving schema information...");
    my %return;

    my $schema =
        (@schemas)
        ? "\n    AND n.nspname IN (" . $self->_array_to_str(@schemas) . ")"
        : '';

    my $query = qq{
SELECT n.nspname AS schema_name,
        pg_catalog.pg_get_userbyid ( n.nspowner ) AS schema_owner,
        pg_catalog.obj_description ( n.oid, 'pg_namespace' ) AS comments
    FROM pg_catalog.pg_namespace n
    WHERE n.nspname NOT LIKE 'pg_%'
        AND n.nspname <> 'information_schema'$schema
    ORDER BY 2
};

    foreach my $row ( $self->_db_query($query) ) {
        my $schema_name = $row->[0];
        $return{$schema_name}{catalog_name} = '';
        $return{$schema_name}{schema_name}  = $row->[0];
        $return{$schema_name}{schema_owner} = $row->[1];
        $return{$schema_name}{comments}     = $row->[2];
    }
    return wantarray ? %return : \%return;
}

sub get_table_columns {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_debug("Extracting table column information ...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'c.relname', $filters );

    my $query = qq{
SELECT n.nspname AS table_schema,
        c.relname AS table_name,
        a.attname AS column_name,
        a.attnum AS ordinal_position,
        pg_catalog.format_type ( a.atttypid, a.atttypmod ) AS data_type,
        CASE
            WHEN a.attnotnull THEN 'N'
            ELSE 'Y'
            END AS is_nullable,
        pg_catalog.pg_get_expr ( ad.adbin, ad.adrelid ) AS data_default,
        pg_catalog.col_description ( a.attrelid, a.attnum ) AS comments
    FROM pg_catalog.pg_class c
    LEFT OUTER JOIN pg_catalog.pg_namespace n
        ON ( n.oid = c.relnamespace )
    LEFT OUTER JOIN pg_catalog.pg_attribute a
        ON ( c.oid = a.attrelid
            AND a.attnum > 0
            AND NOT a.attisdropped )
    LEFT OUTER JOIN pg_attrdef ad
        ON ( a.attrelid = ad.adrelid
            AND a.attnum = ad.adnum )
    WHERE c.relkind IN ( 'v', 'r', 'f', 'm' )
        AND n.nspname = ? $table_filter
    ORDER BY a.attnum
};

    my $sth = $self->_db_prepare($query);
    # Because `my @column_names = @{$sth->{NAME_lc}};` isn't working
    my @column_names =
        (qw( table_schema table_name column_name ordinal_position data_type is_nullable data_default comments ));

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $table_name  = $row->[1];
        my $column_name = $row->[2];
        $return{$table_name}{columns}{$column_name}{ $column_names[$_] } = $row->[$_] for ( 0 .. $#column_names );
        push @{ $return{$table_name}{column_names} }, $column_name;

        my $data_type = $return{$table_name}{columns}{$column_name}{data_type};
        push @{ $return{$table_name}{data_types} }, $data_type;

        # split the data type into length, scale, and precision
        my ( $dt, $data_precision, $data_scale ) = split /[(,)]/, $data_type;
        $return{$table_name}{columns}{$column_name}{data_type}      = $dt;
        $return{$table_name}{columns}{$column_name}{data_precision} = $data_precision;
        $return{$table_name}{columns}{$column_name}{data_scale}     = $data_scale;
    }
    return wantarray ? %return : \%return;
}

sub get_tables {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'c.relname', $filters );

    my $query = qq{
SELECT '' AS catalog_name,
        n.nspname AS table_schema,
        pg_catalog.pg_get_userbyid ( c.relowner ) AS table_owner,
        CASE c.relkind
            WHEN 'f' THEN 'FOREIGN TABLE'
            WHEN 'm' THEN 'MATERIALIZED VIEW'
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
            END AS table_type,
        c.relname AS table_name,
        '' AS tablespace_name,
        s.n_live_tup AS row_count,
        count ( a.attname ) AS column_count,
        pg_catalog.obj_description ( c.oid, 'pg_class' ) AS comments,
        CASE c.relkind
            WHEN 'v' THEN pg_catalog.pg_get_viewdef ( c.oid, true )
            ELSE NULL
            END AS query
    FROM pg_catalog.pg_class c
    LEFT OUTER JOIN pg_catalog.pg_namespace n
        ON ( n.oid = c.relnamespace )
    LEFT OUTER JOIN pg_catalog.pg_attribute a
        ON ( c.oid = a.attrelid
            AND a.attnum > 0
            AND NOT a.attisdropped )
    LEFT OUTER JOIN pg_catalog.pg_stat_all_tables s
        ON ( c.oid = s.relid )
    WHERE c.relkind IN ( 'v', 'r', 'f', 'm' )
        AND n.nspname = ? $table_filter
    GROUP BY n.nspname,
        c.relowner,
        c.relkind,
        c.relname,
        s.n_live_tup,
        c.oid
    ORDER BY n.nspname,
        c.relname
};

    my $sth = $self->_db_prepare($query);
    # Because `my @column_names = @{$sth->{NAME_lc}};` isn't working
    my @column_names = (
        qw( catalog_name table_schema table_owner table_type table_name tablespace_name row_count column_count comments query )
    );

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $table_name = $row->[4];
        foreach my $idx ( 0 .. $#column_names ) {
            $return{$table_name}{ $column_names[$idx] } = $row->[$idx];
        }
    }

    # Add foreign data wrapper details
    $query = qq{
SELECT n.nspname AS table_schema,
        c.relname AS table_name,
        s.srvname AS server,
        array_to_string ( array (
                SELECT quote_ident ( option_name ) || ' ' || quote_literal ( option_value )
                    FROM pg_options_to_table ( s.srvoptions || ft.ftoptions )
            ),
            ', ') AS table_options
    FROM pg_catalog.pg_foreign_table ft
    INNER JOIN pg_catalog.pg_class c
        ON c.oid = ft.ftrelid
    INNER JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
    INNER JOIN pg_catalog.pg_foreign_server s
        ON s.oid = ft.ftserver
    WHERE n.nspname = ? $table_filter
};

    $sth          = $self->_db_prepare($query);
    @column_names = ( qw( table_schema table_name server table_options ) );

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $table_name    = $row->[1];
        my $table_options = $row->[3];
        my $comments      = $return{$table_name}{comments} || '';
        $return{$table_name}{comments} = $comments . ' [ fdw options: (' . $table_options . ')]';
    }

    return wantarray ? %return : \%return;
}

sub get_types {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving type information...");
    my %return;
    my $schema = $self->{schema};

    return undef;

    my $query = qq{


SELECT n.nspname as "Schema",
  pg_catalog.format_type(t.oid, NULL) AS "Name",
  t.typname AS "Internal name",
  CASE WHEN t.typrelid != 0
      THEN CAST('tuple' AS pg_catalog.text)
    WHEN t.typlen < 0
      THEN CAST('var' AS pg_catalog.text)
    ELSE CAST(t.typlen AS pg_catalog.text)
  END AS "Size",
  pg_catalog.array_to_string(
      ARRAY(
		     SELECT e.enumlabel
          FROM pg_catalog.pg_enum e
          WHERE e.enumtypid = t.oid
          ORDER BY e.oid
      ),
      E'\n'
  ) AS "Elements",
  pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  AND (t.typname ~ '^(text)$'
        OR pg_catalog.format_type(t.oid, NULL) ~ '^(text)$')
  AND pg_catalog.pg_type_is_visible(t.oid)
ORDER BY 1, 2;





    SELECT n.nspname AS schema,
            pg_catalog.format_type ( t.oid, NULL ) AS name,
            t.typname AS internal_name,
            CASE
                WHEN t.typrelid != 0
                THEN CAST ( 'tuple' AS pg_catalog.text )
                WHEN t.typlen < 0
                THEN CAST ( 'var' AS pg_catalog.text )
                ELSE CAST ( t.typlen AS pg_catalog.text )
            END AS size,
            pg_catalog.array_to_string (
                ARRAY( SELECT e.enumlabel
                        FROM pg_catalog.pg_enum e
                        WHERE e.enumtypid = t.oid
                        ORDER BY e.oid
                    ), E'\n'
                ) AS elements,
            pg_catalog.obj_description ( t.oid, 'pg_type' ) AS description
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n
            ON n.oid = t.typnamespace
        WHERE ( t.typrelid = 0
                OR ( SELECT c.relkind = 'c'
                        FROM pg_catalog.pg_class c
                        WHERE c.oid = t.typrelid
                    )
            )
            AND NOT EXISTS
                ( SELECT 1
                    FROM pg_catalog.pg_type el
                    WHERE el.oid = t.typelem
                        AND el.typarray = t.oid
                )
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
            AND pg_catalog.pg_type_is_visible ( t.oid )
        ORDER BY 1, 2;



SELECT n.nspname as object_schema,
  pg_catalog.format_type(t.oid, NULL) AS object_name,
  --pg_catalog.pg_get_userbyid(c.relowner) AS object_owner,
  pg_catalog.obj_description(t.oid, 'pg_type') as source
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
      AND n.nspname = ?
  AND pg_catalog.pg_type_is_visible(t.oid)
ORDER BY 1, 2;
};

    # TODO : test this
    # TODO : owner
    # TODO : Comment

    my $sth = $self->_db_prepare($query);
    # Because `my @column_names = @{$sth->{NAME_lc}};` isn't working
    my @column_names = (qw( object_schema object_name source));

    $sth->execute($schema) || $self->_log_fatal( $sth->errstr );
    foreach my $row ( @{ $sth->fetchall_arrayref } ) {
        my $object_name = $row->[1];
        foreach my $idx ( 0 .. $#column_names ) {
            $return{$object_name}{ $column_names[$idx] } = $row->[$idx];
        }
    }
    return wantarray ? %return : \%return;
}

sub get_unique_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving unique constraint information...");
    my %return;
    my $schema = $self->{schema};
    my $table_filter = $self->_get_table_filter( 'r.relname', $filters );

    my $query = qq{
SELECT nr.nspname AS table_schema,
        r.relname AS table_name,
        c.conname AS constraint_name,
        split_part ( split_part ( pg_get_constraintdef ( c.oid ), '(', 2 ), ')', 1 ) AS column_names,
        d.description AS comments
    FROM pg_class r
    INNER JOIN pg_namespace nr
        ON ( nr.oid = r.relnamespace )
    INNER JOIN pg_constraint c
        ON ( c.conrelid = r.oid )
    INNER JOIN pg_namespace nc
        ON ( nc.oid = c.connamespace )
    LEFT OUTER JOIN pg_description d
        ON ( d.objoid = c.oid )
    WHERE r.relkind = 'r'
        AND c.contype = 'u'
        AND nr.nspname = ? $table_filter
    ORDER BY r.relname,
        c.conname
};

    foreach my $row ( $self->_db_query( $query, $schema ) ) {
        my $table_name      = $row->[1];
        my $constraint_name = $row->[2];
        $return{$table_name}{$constraint_name}{table_schema}    = $row->[0];
        $return{$table_name}{$constraint_name}{table_name}      = $table_name;
        $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
        @{ $return{$table_name}{$constraint_name}{column_names} } = split ',', $row->[3];
        $return{$table_name}{$constraint_name}{status}   = 'Enabled';
        $return{$table_name}{$constraint_name}{comments} = $row->[4];
    }
    return wantarray ? %return : \%return;
}

1;
__END__
