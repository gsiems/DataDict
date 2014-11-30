package DataDict::Config;
use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use File::Copy;
use File::Spec;
use Carp();

use POSIX qw(locale_h);
setlocale( LC_NUMERIC, "C" );

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@EXPORT = qw(
    read_config
    get_value
    get_table_filter
);

@ISA = qw(Exporter);

sub read_config {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    # Grab the calling args
    Getopt::Long::Configure(qw(bundling));
    my %opts = ();
    # TODO : list of valid command line args
    GetOptions(
        "l=s"                  => \$opts{log_file},
        "log_file=s"           => \$opts{log_file},
        "log_level=s"          => \$opts{log_level},
        "append_log_file"      => \$opts{append_log_file},
        "c=s"                  => \$opts{config},
        "config=s"             => \$opts{config},
        "data_limit=i"         => \$opts{data_limit},
        "f=s"                  => \$opts{output_format},
        "output_format=s"      => \$opts{output_format},
        "target_db=s"          => \$opts{target_db},
        "nojs"                 => \$opts{no_js},
        "show_sql"             => \$opts{show_sql},
        "schemas=s"            => \$opts{schemas},
        "include_tables=s"     => \$opts{include_tables},
        "exclude_tables=s"     => \$opts{exclude_tables},
        "t=s"                  => \$opts{target_dir},
        "target_dir=s"         => \$opts{target_dir},
        "directory_per_schema" => \$opts{directory_per_schema},
        "file_per_object"      => \$opts{file_per_object},
        "h"                    => \$opts{help},
        "help"                 => \$opts{help},
        "v"                    => \$opts{verbose},
        "verbose"              => \$opts{verbose},
        "V"                    => \$opts{version},
        "version"              => \$opts{version},
    );

    # Determine the config file
    $self->{config_file} =
        ( exists $opts{config} )
        ? $opts{config}
        : '';    # TODO: default?

    unless ( $self->{config_file} ) {
        Carp::croak("No configuration file specified or found.\n.");
    }

    unless ( -f $self->{config_file} ) {
        Carp::croak("Invalid configuration file specified. File does not exist or is not a file.\n.");
    }

    # Read the config file
    if ( $self->{config_file} ) {
        my @ary = split /\n/, $self->_slurp_file( $self->{config_file} );

        # Cull the empty lines and comments
        @ary = grep { $_ !~ m/^\s*#/ } grep { $_ !~ m/^\s*$/ } @ary;

        my $last_key   = '';
        my $last_value = '';

        foreach (@ary) {
            my ( $key, $value ) = $_ =~ m/^\s*([^\s]+)\s*=\s*(.+)/;
            if ($key) {    # New config item
                ( $last_key, $last_value ) = ( $key, $value );
                $value =~ s/\\$//;
                $self->{$key} = $value;
            }
            elsif ( $last_value =~ m/\\$/ ) {    # Continuation of current config item
                my ($value) = $_ =~ m/^\s*(.+)$/;
                $last_value = $value;
                $value =~ s/\\$//;
                $self->{$last_key} .= ' ' . $value;
            }
        }
    }

    # Append/replace the config file entries with the calling opts
    foreach my $key ( keys %opts ) {
        my $value = $opts{$key};
        $self->{$key} = $value if ( defined $value );
    }

    # Logging
    my %logger_args;
    foreach my $key (qw(log_level append_log_file log_file)) {
        if ( exists $self->{$key} ) {
            $logger_args{$key} = $self->{$key};
        }
    }
    $self->{logger} = DataDict::Logger->new_logger(%logger_args);

    my ( undef, $bin_dir, $bin_file ) = File::Spec->splitpath( File::Spec->rel2abs($0) );
    my $cur_dir = File::Spec->rel2abs( File::Spec->curdir() );
    $self->{bin_dir}       = $bin_dir;
    $self->{bin_file}      = $bin_file;
    $self->{cur_dir}       = $cur_dir;
    $self->{run_timestamp} = localtime();

    my $base_dir = $bin_dir;
    $base_dir =~ s|bin[/\\]$||;
    $self->{base_dir} = $base_dir;

    unless ( exists $self->{template_dir} ) {
        my $template_path = File::Spec->catfile( $base_dir, 'templates' );
        #        $template_path =~ s|bin[/\\]$|templates|;
        $self->{template_dir} = File::Spec->rel2abs($template_path);
    }

    # Set parameter defaults if needed.
    $self->{data_limit} ||= 10000;
    $self->{target_dir} ||= $self->{cur_dir};

    # $self->get_value('dir_per_schema')  || 1;

    # Config options that only make sense for pg_dump output (ora2pg) should probably be relegated to the config file.
    # TODO : standard_conforming_strings
    # TODO : rename_schemas {old_name:new_name[, old_name2:new_name2[, ...]]}
    # TODO : rename_tables  {old_name:new_name[, old_name2:new_name2[, ...]]}
    # TODO : rename_columns {tab_name(old_name:new_name[, old_name2:new_name2[, ...]])}
    # TODO : extract_views_as { view, table } If set to 'table' then extracts oracle views as tables. Default is to extract oracle views as views.
    # TODO : extract_mviews_as { view, table, mview } If set to 'table' then extracts oracle materialized views as tables. Default is to extract oracle views as views.
    # TODO : preserve_pkey_names { 1, 0 }
    # TODO : exclude_binary ? or include_binary ? which should be default ?

    # STDOUT or to file(s)?
    # TODO : directory_per_schema ? or should that be the default
    # TODO : file_per_object ? when does this make sense? pg_dump, insert only ? csv, tab, etc. export requires separate files

=pod

=item owner { source, other, undefined }

Indicates how to deal with ownership of tables, views etc. If set to "source" then uses the owner from the source database to set ownership; otherwise uses the specified owner to set ownership. If not specified then does not set ownership. Default is to not set ownership.

=item preserve_case { 1, 0 }

Specifies whether or not to preserve the capitalization of the names of tables, views, etc.

=item rename_tables

Rename specified tables during export. Comma separated list of
old_name:new_name pairs of tables, views to rename as part of an export.
(old_name1:dest_name1, old_name2:dest_name2, ...)


=cut

    return $self;
}

sub get_keys {
    my ($self) = @_;
    my @keys = keys %{$self};
    return @keys;
}

#sub get_column_filter {
#    my ($self, $schema) = @_;
#    my %return;
#
#    my @schema_include = ();
#    my @schema_exclude = ();
#    my @global_include = ();
#    my @global_exclude = ();
#
#    if ($schema && exists $self->{$schema . '-include_columns'}) {
#        @schema_include = split /\s*,\s*/, $self->{$schema . '-include_columns'};
#    }
#    if ($schema && exists $self->{$schema . '-exclude_columns'}) {
#        @schema_exclude = split /\s*,\s*/, $self->{$schema . '-exclude_columns'};
#    }
#    if (exists $self->{'include_columns'}) {
#        @global_include = split /\s*,\s*/, $self->{'include_columns'};
#    }
#    if (exists $self->{'exclude_columns'}) {
#        @global_exclude = split /\s*,\s*/, $self->{'exclude_columns'};
#    }
#
#    # Combine and try to preserve order (in case it matters at some point).
#    # In the event that a table name shows up in more thatn on place:
#    #   1. schema includes take priority, followed by
#    #   2. schema excludes, followed by
#    #   3. global includes, followed by
#    #   4. global excludes
#    my %seen = ();
#    foreach my $table_name (@schema_include) {
#        next if (exists $seen{$table_name});
#        my $tag = ($table_name =~ m/\%/) ? 'LIKE' : 'IN';
#        push @{$return{$tag}}, $table_name;
#        $seen{$table_name} = $tag;
#    }
#    foreach my $table_name (@schema_exclude) {
#        next if (exists $seen{$table_name});
#        my $tag = ($table_name =~ m/\%/) ? 'NOT LIKE' : 'NOT IN';
#        push @{$return{$tag}}, $table_name;
#        $seen{$table_name} = $tag;
#    }
#    foreach my $table_name (@global_include) {
#        next if (exists $seen{$table_name});
#        my $tag = ($table_name =~ m/\%/) ? 'LIKE' : 'IN';
#        push @{$return{$tag}}, $table_name;
#        $seen{$table_name} = $tag;
#    }
#    foreach my $table_name (@global_exclude) {
#        next if (exists $seen{$table_name});
#        my $tag = ($table_name =~ m/\%/) ? 'NOT LIKE' : 'NOT IN';
#        push @{$return{$tag}}, $table_name;
#        $seen{$table_name} = $tag;
#    }
#
#    return wantarray ? %return : \%return;
#}

sub get_table_filter {
    my ( $self, $schema ) = @_;
    my %return;

    my @schema_include = ();
    my @schema_exclude = ();
    my @global_include = ();
    my @global_exclude = ();

    if ( $schema && exists $self->{ $schema . '-include_tables' } ) {
        @schema_include = split /\s*,\s*/, $self->{ $schema . '-include_tables' };
    }
    if ( $schema && exists $self->{ $schema . '-exclude_tables' } ) {
        @schema_exclude = split /\s*,\s*/, $self->{ $schema . '-exclude_tables' };
    }
    if ( exists $self->{'include_tables'} ) {
        @global_include = split /\s*,\s*/, $self->{'include_tables'};
    }
    if ( exists $self->{'exclude_tables'} ) {
        @global_exclude = split /\s*,\s*/, $self->{'exclude_tables'};
    }

    # Combine and try to preserve order (in case it matters at some point).
    # In the event that a table name shows up in more thatn on place:
    #   1. schema includes take priority, followed by
    #   2. schema excludes, followed by
    #   3. global includes, followed by
    #   4. global excludes
    my %seen = ();
    foreach my $table_name (@schema_include) {
        next if ( exists $seen{$table_name} );
        my $tag = ( $table_name =~ m/\%/ ) ? 'LIKE' : 'IN';
        push @{ $return{$tag} }, $table_name;
        $seen{$table_name} = $tag;
    }
    foreach my $table_name (@schema_exclude) {
        next if ( exists $seen{$table_name} );
        my $tag = ( $table_name =~ m/\%/ ) ? 'NOT LIKE' : 'NOT IN';
        push @{ $return{$tag} }, $table_name;
        $seen{$table_name} = $tag;
    }
    foreach my $table_name (@global_include) {
        next if ( exists $seen{$table_name} );
        my $tag = ( $table_name =~ m/\%/ ) ? 'LIKE' : 'IN';
        push @{ $return{$tag} }, $table_name;
        $seen{$table_name} = $tag;
    }
    foreach my $table_name (@global_exclude) {
        next if ( exists $seen{$table_name} );
        my $tag = ( $table_name =~ m/\%/ ) ? 'NOT LIKE' : 'NOT IN';
        push @{ $return{$tag} }, $table_name;
        $seen{$table_name} = $tag;
    }

    return wantarray ? %return : \%return;
}

sub get_value {
    my ( $self, $key ) = @_;
    if ( exists $self->{$key} ) {
        return $self->{$key};
    }
    return undef;
}

sub _slurp_file {
    my $self = shift;
    local ( *ARGV, $/ );
    @ARGV = shift;
    <>;
}

1;
__END__
