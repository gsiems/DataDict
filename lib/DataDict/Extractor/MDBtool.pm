package DataDict::Extractor::MDBtool;
use base 'DataDict::Extractor';

use strict;
use warnings;

use Data::Dumper;

=pod

Use mdbtools to extract Access database items.

=cut

sub _post_init {
    my ($self) = @_;

    #database_dsn
    my ( undef, $file ) = split /[=,]/, $self->{database_dsn};
    if ( -f $file ) {
        $self->{db_filename} = $file;
    }
    else {
        Carp::croak("Specified database file '$file' does not exist.\n");
    }
}

sub _post_set_schema {
    my ($self) = @_;
    if ( exists $self->{table_cache} ) {
        delete $self->{table_cache};
    }

    my @tables = $self->_table_list();
    foreach my $table_name (@tables) {
        my %table = $self->_table_ddl($table_name);
        $self->{table_cache}{$table_name} = \%table;
    }
}

sub get_db_comment {
    my ($self) = @_;
    $self->{logger}->log_info("Retrieving database comment information...");
    my $comment = $self->{database_comment} || '';
    return $comment;
}

sub _table_list {
    my ($self) = @_;
    my @tables = sort `mdb-tables -1 $self->{db_filename}`;
    chomp @tables;
    return @tables;
}

sub _table_ddl {
    my ( $self, $table_name ) = @_;
    my %return;

    my @ddl = `mdb-schema --not-null --no-indexes --no-relations -T "$table_name" $self->{db_filename}`;
    my @columns = grep { $_ =~ m/^\s+\[/ } @ddl;
    chomp @columns;
    $return{column_count} = scalar @columns;
    @{ $return{column_clauses} } = @columns;

    @ddl =
        `mdb-schema --not-null --default-values --indexes --relations --not-empty -T "$table_name" $self->{db_filename} postgres`;
    chomp @ddl;
    @{ $return{indexes} }        = grep { $_ =~ m/^CREATE INDEX/ } @ddl;
    @{ $return{unique_indexes} } = grep { $_ =~ m/^CREATE UNIQUE INDEX/ } @ddl;
    ( $return{primary_key} ) = grep { $_ =~ m/ PRIMARY KEY \(/ } @ddl;
    @{ $return{foreign_keys} }      = grep { $_ =~ m/ FOREIGN KEY \(/ } @ddl;
    @{ $return{check_constraints} } = grep { $_ =~ m/^ALTER .+ ADD CHECK \(/ } @ddl;

    # Row counts:
    # echo 'select <column_name> from <table_name>'  | mdb-sql <db_file> | grep 'Rows retrieved'

    # Now to get the table comment...
    if ( grep { $_ =~ m/COMMENT ON TABLE/ } @ddl ) {
        my $comment = '';
        foreach my $line (@ddl) {
            if ( $line =~ m/COMMENT ON TABLE/ ) {
                my ( undef, $tmp ) = split /" IS '/, $line, 2;
                $comment = $line;
            }
            elsif ($comment) {
                $comment .= ' ' . $line;
            }
            if ( $comment && $line =~ m/';\s*$/ ) {
                $comment =~ s/';\s*$//;
                last;
            }
        }
        $return{table_comment} = $comment;
    }

    # Column comments...
    if ( grep { $_ =~ m/COMMENT ON COLUMN/ } @ddl ) {
        my $comment;
        foreach my $line (@ddl) {
            if ( $line =~ m/COMMENT ON COLUMN/ ) {
                $comment = $line;
            }
            elsif ($comment) {
                $comment .= ' ' . $line;
            }
            if ( $comment && $line =~ m/';\s*$/ ) {
                push @{ $return{column_comments} }, $comment;
                $comment = '';
            }
        }
    }
    return %return;
}

sub get_objects {
    my ( $self, $object_type, @args ) = @_;

    $object_type ||= 'UNKNOWN';

    if ( uc $object_type eq 'SCHEMA' )           { return $self->get_schemas(@args); }
    if ( uc $object_type eq 'TABLE' )            { return $self->get_tables(@args); }
    if ( uc $object_type eq 'COLUMN' )           { return $self->get_table_columns(@args); }
    if ( uc $object_type eq 'CHECK CONSTRAINT' ) { return $self->get_check_constraints(@args); }
    if ( uc $object_type eq 'CHILD KEY' )        { return $self->get_child_keys(@args); }
    if ( uc $object_type eq 'FOREIGN KEY' )      { return $self->get_foreign_keys(@args); }
    if ( uc $object_type eq 'INDEX' )            { return $self->get_indexes(@args); }
    if ( uc $object_type eq 'PRIMARY KEY' )      { return $self->get_primary_keys(@args); }

    $self->{logger}->log_warning("No extraction routine available for $object_type objects...");

    return undef;
}

sub get_schemas {
    my ( $self, @schemas ) = @_;
    $self->{logger}->log_info("Retrieving schema information...");
    my %return;
    my $schema_name = 'public';

    $return{$schema_name}{catalog_name} = undef;
    $return{$schema_name}{schema_name}  = $schema_name;
    $return{$schema_name}{schema_owner} = undef;
    $return{$schema_name}{comments}     = undef;

    return wantarray ? %return : \%return;
}

sub get_tables {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving table information...");
    my %return;
    my $schema = $self->{schema};

    my @tables   = sort keys %{ $self->{table_cache} };
    my @ret_cols = (
        qw( catalog_name table_schema table_owner table_type table_name tablespace_name row_count column_count comments query )
    );
    foreach my $table_name (@tables) {
        $return{$table_name}{$_} = undef for (@ret_cols);

        $return{$table_name}{table_schema} = $schema;
        $return{$table_name}{table_type}   = 'TABLE';
        $return{$table_name}{column_count} = $self->{table_cache}{$table_name}{column_count};

        my $table_comment = $self->{table_cache}{$table_name}{table_comment} || '';
        $table_comment =~ s/^.+" IS '?//;
        $table_comment =~ s/';\s*$//;
        $return{$table_name}{comments} = $table_comment;
    }
    return wantarray ? %return : \%return;
}

sub get_table_columns {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_debug("Extracting table column information...");
    my %return;
    my $schema = $self->{schema};

    my @tables = sort keys %{ $self->{table_cache} };
    my @ret_cols =
        (qw( table_schema table_name column_name ordinal_position data_type is_nullable data_default comments ));
    foreach my $table_name (@tables) {
        my $ordinal_position = 0;
        foreach my $column ( @{ $self->{table_cache}{$table_name}{column_clauses} } ) {
            $ordinal_position++;

            my ( undef, $column_name, $remainder ) = split /[\[\]]/, $column;
            $remainder =~ s/,\s*$//;
            my @ary = split /(NOT NULL|NULL)/, $remainder;
            my $data_type = shift @ary;
            $data_type =~ s/^\s+//;
            $data_type =~ s/\s+$//;

            my $is_nullable = ( grep { $_ eq 'NOT NULL' } @ary ) ? 'N' : 'Y';

            my ($data_default) = grep { $_ =~ m/DEFAULT/ } @ary;
            if ($data_default) {
                $data_default =~ s/DEFAULT //;
            }
            $data_default ||= '';

            push @{ $return{$table_name}{column_names} }, $column_name;
            push @{ $return{$table_name}{data_types} },   $data_type;

            # split the data type into length, scale, and precision
            my ( $dt, $data_precision, $data_scale ) = split /[(,)]/, $data_type;
            $return{$table_name}{columns}{$column_name}{table_schema}     = $schema;
            $return{$table_name}{columns}{$column_name}{table_name}       = $table_name;
            $return{$table_name}{columns}{$column_name}{column_name}      = $column_name;
            $return{$table_name}{columns}{$column_name}{data_type}        = $dt;
            $return{$table_name}{columns}{$column_name}{data_precision}   = $data_precision;
            $return{$table_name}{columns}{$column_name}{data_scale}       = $data_scale;
            $return{$table_name}{columns}{$column_name}{ordinal_position} = $ordinal_position;
            $return{$table_name}{columns}{$column_name}{is_nullable}      = $is_nullable;
            $return{$table_name}{columns}{$column_name}{data_default}     = $data_default;

            my ($column_comment) =
                grep { $_ =~ m/"$column_name" IS '/ } @{ $self->{table_cache}{$table_name}{column_comments} };
            $column_comment ||= '';
            $column_comment =~ s/^.+" IS '?//;
            $column_comment =~ s/';\s*$//;
            $return{$table_name}{columns}{$column_name}{comments} = $column_comment;
        }
    }
    return wantarray ? %return : \%return;
}

sub get_primary_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving primary key information ...");
    my %return;
    my $schema = $self->{schema};

    my @tables = sort keys %{ $self->{table_cache} };
    foreach my $table_name (@tables) {
        # ALTER TABLE "temp_TPDevSites" ADD CONSTRAINT "temp_TPDevSites_pkey" PRIMARY KEY ("SiteNum");
        my $pk = $self->{table_cache}{$table_name}{primary_key} || '';
        next unless ($pk);
        my ($constraint_name) = $pk =~ m/ADD CONSTRAINT "(.+)" PRIMARY KEY/;
        my ( undef, $cons_columns ) = split /[()]/, $pk;
        $cons_columns =~ s/^"//;
        $cons_columns =~ s/"$//;
        my @columns = split /",\s*"/, $cons_columns;

        $return{$table_name}{table_schema}    = $schema;
        $return{$table_name}{table_name}      = $table_name;
        $return{$table_name}{constraint_name} = $constraint_name;
        $return{$table_name}{status}          = 'Enabled';
        $return{$table_name}{comments}        = '';

        foreach my $idx ( 0 .. $#columns ) {
            push @{ $return{$table_name}{column_names} },     $columns[$idx];
            push @{ $return{$table_name}{column_positions} }, $idx + 1;
        }
    }
    return wantarray ? %return : \%return;
}

sub get_indexes {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving index information...");
    my %return;
    my $schema = $self->{schema};

    my @tables = sort keys %{ $self->{table_cache} };
    foreach my $table_name (@tables) {
        my @indexes = @{ $self->{table_cache}{$table_name}{indexes} };
        foreach my $idx (@indexes) {
            my ($index_name) = $idx =~ m/INDEX "(.+)" ON/;
            my ( undef, $idx_columns ) = split /[()]/, $idx;
            $idx_columns =~ s/^"//;
            $idx_columns =~ s/"$//;
            my @columns = split /",\s*"/, $idx_columns;
            next unless ( scalar @columns );

            $return{$table_name}{$index_name}{table_schema} = $schema;
            $return{$table_name}{$index_name}{table_name}   = $table_name;
            $return{$table_name}{$index_name}{index_name}   = $index_name;
            @{ $return{$table_name}{$index_name}{column_names} } = @columns;
            $return{$table_name}{$index_name}{is_unique} = 'N';
            $return{$table_name}{$index_name}{comments}  = '';
            @{ $return{$table_name}{$index_name}{decends} } = map { '' } @columns;
        }

        my @uniq_indexes = @{ $self->{table_cache}{$table_name}{unique_indexes} };
        foreach my $idx (@uniq_indexes) {
            my ($index_name) = $idx =~ m/INDEX "(.+)" ON/;
            my ( undef, $idx_columns ) = split /[()]/, $idx;
            $idx_columns =~ s/^"//;
            $idx_columns =~ s/"$//;
            my @columns = split /",\s*"/, $idx_columns;
            next unless ( scalar @columns );

            $return{$table_name}{$index_name}{table_schema} = $schema;
            $return{$table_name}{$index_name}{table_name}   = $table_name;
            $return{$table_name}{$index_name}{index_name}   = $index_name;
            @{ $return{$table_name}{$index_name}{column_names} } = @columns;
            $return{$table_name}{$index_name}{is_unique} = 'Y';
            $return{$table_name}{$index_name}{comments}  = '';
            @{ $return{$table_name}{$index_name}{decends} } = map { '' } @columns;
        }
    }
    return wantarray ? %return : \%return;
}

sub get_foreign_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving foreign key information...");
    my %return;
    my $schema = $self->{schema};

    my @tables = sort keys %{ $self->{table_cache} };
    foreach my $table_name (@tables) {
        my @foreign_keys = @{ $self->{table_cache}{$table_name}{foreign_keys} };
        foreach my $fk (@foreign_keys) {
            next unless ($fk);

            my ($constraint_name) = $fk =~ m/ADD CONSTRAINT "(.+)" FOREIGN KEY/;
            my ($cons_columns)    = $fk =~ m/FOREIGN KEY \("(.+)"\) REFERENCES/;
            my ($r_table_name)    = $fk =~ m/REFERENCES "(.+)"\s*\(/;
            my ($r_cons_cols)     = $fk =~ m/REFERENCES ".+"\s*\("(.+)"\)/;
            my ($constraint_rule) = $fk =~ m/\)\s*([^)]+);\s*$/;

            $return{$table_name}{$constraint_name}{table_schema}    = $schema;
            $return{$table_name}{$constraint_name}{table_name}      = $table_name;
            $return{$table_name}{$constraint_name}{constraint_name} = $constraint_name;
            @{ $return{$table_name}{$constraint_name}{column_names} } = split '",\s*"', $cons_columns;
            $return{$table_name}{$constraint_name}{r_table_schema} = $schema;
            $return{$table_name}{$constraint_name}{r_table_name}   = $r_table_name;
            @{ $return{$table_name}{$constraint_name}{r_column_names} } = split '",\s*"', $r_cons_cols;
            $return{$table_name}{$constraint_name}{constraint_rule} = $constraint_rule;
            $return{$table_name}{$constraint_name}{status}          = 'Enabled';
            $return{$table_name}{$constraint_name}{comments}        = '';
        }
    }
    return wantarray ? %return : \%return;
}

sub get_child_keys {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Retrieving child key information...");
    my %return;
    my $schema = $self->{schema};

    my @tables = sort keys %{ $self->{table_cache} };
    foreach my $r_table_name (@tables) {
        my @foreign_keys = @{ $self->{table_cache}{$r_table_name}{foreign_keys} };
        foreach my $fk (@foreign_keys) {
            next unless ($fk);

            my ($constraint_name) = $fk =~ m/ADD CONSTRAINT "(.+)" FOREIGN KEY/;
            my ($r_cons_cols)     = $fk =~ m/FOREIGN KEY \("(.+)"\) REFERENCES/;
            my ($table_name)      = $fk =~ m/REFERENCES "(.+)"\s*\(/;
            my ($cons_columns)    = $fk =~ m/REFERENCES ".+"\s*\("(.+)"\)/;
            my ($constraint_rule) = $fk =~ m/\)\s*([^)]+);\s*$/;

            next unless ( $r_cons_cols && $cons_columns && $constraint_name );

            $return{$table_name}{$schema}{$constraint_name}{table_schema}    = $schema;
            $return{$table_name}{$schema}{$constraint_name}{table_name}      = $table_name;
            $return{$table_name}{$schema}{$constraint_name}{constraint_name} = $constraint_name;
            @{ $return{$table_name}{$schema}{$constraint_name}{column_names} } = split '",\s*"', $cons_columns;
            $return{$table_name}{$schema}{$constraint_name}{r_table_schema} = $schema;
            $return{$table_name}{$schema}{$constraint_name}{r_table_name}   = $r_table_name;
            @{ $return{$table_name}{$schema}{$constraint_name}{r_column_names} } = split '",\s*"', $r_cons_cols;
            $return{$table_name}{$schema}{$constraint_name}{constraint_rule} = $constraint_rule;
            $return{$table_name}{$schema}{$constraint_name}{status}          = 'Enabled';
            $return{$table_name}{$schema}{$constraint_name}{comments}        = '';
        }
    }
    return wantarray ? %return : \%return;
}

#ALTER TABLE "Sites" ADD CHECK ("LEG_DIST" <>'');
sub get_check_constraints {
    my ( $self, $filters ) = @_;
    $self->{logger}->log_info("Extracting check_constraint information...");
    my %return;
    my $schema = $self->{schema};
    my $i      = 0;
    my @tables = sort keys %{ $self->{table_cache} };
    foreach my $table_name (@tables) {
        my @check_constraints = @{ $self->{table_cache}{$table_name}{check_constraints} };
        foreach my $cons (@check_constraints) {
            next unless ($cons);
            my ($search_condition) = $cons =~ m/ADD CHECK \((.+)\);\s*$/;
            $i++;
            $return{$table_name}{$i}{table_schema}     = $schema;
            $return{$table_name}{$i}{table_name}       = $table_name;
            $return{$table_name}{$i}{constraint_name}  = '';
            $return{$table_name}{$i}{search_condition} = $search_condition;
            $return{$table_name}{$i}{status}           = 'Enabled';
            $return{$table_name}{$i}{comments}         = '';
        }
    }
    return wantarray ? %return : \%return;
}

1;
