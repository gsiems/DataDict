package DataDict::Extractor;
use strict;
use warnings;
use DBI;
use Carp();
use Data::Dumper;

use POSIX qw(locale_h);
setlocale( LC_NUMERIC, "C" );

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@EXPORT = qw(
    new_extractor
    set_schema
    compile_schema
    extract_data
    extract_done
    get_objects
    get_db_encoding
    get_db_version
    get_db_comment
    get_table_dependency_order
);

@ISA = qw(Exporter);

# TODO: prepare and extract user query as table
# TODO : use $sql = $dbh->quote_identifier( $name );

sub new_extractor {
    my $this = shift;

    my %args = @_;

    my %aoi;
    if ( exists $args{config} ) {
        my $config   = $args{config};
        my @cfg_keys = $config->get_keys();
        $aoi{$_} = $config->get_value($_) for (@cfg_keys);
    }

    foreach my $key ( keys %aoi ) {
        if ( exists $args{$key} ) {
            $aoi{$key} = $args{$key};
        }
    }

    my $dsn = $aoi{database_dsn} || Carp::croak("No database_dsn specified\n.");
    my $engine = ( split ':', $dsn )[1];
    $aoi{engine} = $engine;

    my $driver_class = "DataDict::Extractor::$engine";
    eval qq{package                     # hide from PAUSE
        DataDict::Extractor::_firesafe; # just in case
        require $driver_class;          # load the driver
    };

    if ($@) {
        my $err = $@;
        Carp::croak("install_driver($driver_class) failed: $err\n");
    }

    my $extractor = $driver_class->_init(%aoi);
    return $extractor;
}

sub _init {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    my %args = @_;
    $self->{$_} = $args{$_} for ( keys %args );
    $self->{logger} ||= DataDict::Logger->new_logger(%args);

    if ( $self->{database_dsn} && $self->{database_user} && $self->{database_password} ) {
        $self->_get_connection( $self->{database_dsn}, $self->{database_user}, $self->{database_password} );
    }

    $self->_post_init();
    return $self;
}

sub _post_init { return undef }

sub set_schema {
    my ( $self, $schema ) = @_;
    $self->{schema} = $schema || undef;
    $self->_post_set_schema();
}

sub _post_set_schema           { return undef }
sub compile_schema             { return undef }
sub extract_data               { return undef }
sub extract_done               { return undef }
sub get_objects                { return undef }
sub get_db_encoding            { return undef }
sub get_db_version             { return undef }
sub get_db_comment             { return undef }
sub get_schema_comment         { return undef }
sub get_table_dependency_order { return undef }

sub _array_to_str {
    my ( $self, @ary ) = @_;
    my $str = join( ',', ( map { "'" . $_ . "'" } @ary ) );
    return $str;
}

sub _get_connection {
    my ( $self, $database_dsn, $database_user, $database_password ) = @_;
    $self->{logger}->log_debug("Getting database connection ($database_dsn)...");

    $self->{dbh} = DBI->connect( $database_dsn, $database_user, $database_password );
    if ($DBI::errstr) {
        $self->_log_fatal($DBI::errstr);
    }
    if ( $database_dsn =~ m/dbi:Oracle/ ) {
        $self->_set_longreadlen();
    }
}

sub _db_do {
    my ( $self, $statement ) = @_;
    $self->{logger}->log_debug("Executing database statement: $statement");

    my $sth = $self->{dbh}->do($statement);
    if ( $self->{dbh}->errstr ) {
        $self->_log_fatal( $self->{dbh}->errstr );
    }
}

sub _db_fetch {
    my ( $self, $query, @parms ) = @_;
    $self->{logger}->log_debug("Executing query:...");

    my $sth = $self->_db_prepare($query);

    if (@parms) {
        $self->{logger}->log_debug( "Query Parameters: '" . join( "','", @parms ) . "'" );
        $sth->execute(@parms);
    }
    else {
        $sth->execute();
    }
    if ( $sth->errstr ) {
        $self->_log_fatal( $sth->errstr );
    }

    my $row = $sth->fetch();
    return $row;
}

sub _db_prepare {
    my ( $self, $query ) = @_;
    unless ($query) {
        $self->_log_fatal("Invalid query string specified.");
    }
    $self->{logger}->log_debug("Preparing query: $query");

    my $sth = $self->{dbh}->prepare($query);
    if ( $self->{dbh}->errstr ) {
        $self->_log_fatal( $self->{dbh}->errstr );
    }
    unless ($sth) {
        $self->_log_fatal("Failed to obtain db statement handle.");
    }
    return $sth;
}

sub _db_query {
    my ( $self, $query, @parms ) = @_;
    $self->{logger}->log_debug("Executing query...");

    my $sth = $self->_db_prepare($query);

    if (@parms) {
        $self->{logger}->log_debug( "Query Parameters: '" . join( "','", @parms ) . "'" );
        $sth->execute(@parms);
    }
    else {
        $sth->execute();
    }
    if ( $sth->errstr ) {
        $self->_log_fatal( $sth->errstr );
    }

    my @results = @{ $sth->fetchall_arrayref() };
    return @results;
}

sub _set_longreadlen {
    my ( $self, $longreadlen ) = @_;
    $self->{dbh}->{'LongReadLen'} = $longreadlen || $self->{longreadlen} || ( 1023 * 1024 );
    $self->{dbh}->{'LongTruncOk'} = 1;
}

sub _clear_longreadlen {
    my ($self) = @_;
    $self->{dbh}->{'LongReadLen'} = 0;
    $self->{dbh}->{'LongTruncOk'} = 1;
}

sub _log_fatal {
    my ( $self, @messages ) = @_;
    if ( $self->{logger} ) {
        $self->{logger}->log_fatal(@messages);
    }
    die "Extract failed.\n";
}

sub _get_table_filter {
    my ( $self, $column_name, $filters ) = @_;
    return '' unless ( defined $filters && $filters );

    my $filter = '';
    foreach my $tag ( 'IN', 'NOT IN' ) {
        if ( exists $filters->{$tag} ) {
            $filter .= "\n    AND $column_name $tag ( " . $self->_array_to_str( @{ $filters->{$tag} } ) . " )";
        }
    }

    if ( exists $filters->{'LIKE'} ) {
        $filter .= "\n    AND (";
        my @ary = map { "$column_name LIKE '$_'" } @{ $filters->{'LIKE'} };
        $filter .= join( ' OR ', @ary );
        $filter .= "\n    )";
    }

    if ( exists $filters->{'NOT LIKE'} ) {
        $filter .= "\n    AND (";
        my @ary = map { "$column_name NOT LIKE '$_'" } @{ $filters->{'LIKE'} };
        $filter .= join( ' AND ', @ary );
        $filter .= "\n    )";
    }

    return $filter;
}

1;
__END__
