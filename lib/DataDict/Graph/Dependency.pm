package DataDict::Graph::Dependency;

use strict;
use warnings;
use POSIX qw(strftime);

use DataDict::Extractor;

our %type_colors = (
    'TABLE'             => "#FFFFE0",    # LightYellow
    'MATERIALIZED VIEW' => "#FFD700",    # Gold
    'VIEW'              => "#DDA0DD",    # Plum
    'FUNCTION'          => "#7FFFD4",    # Aquamarine
    'PACKAGE'           => "#E0FFFF",    # LightCyan
    'PROCEDURE'         => "#87CEFA",    # LightSkyBlue
    'SEQUENCE'          => "#FFFF00",    # Yellow
    'DEFAULT'           => "#F5F5F5",    # Grey
);

our %type_shapes = (
    'TABLE'             => "rectangle",
    'MATERIALIZED VIEW' => "rectangle",
    'VIEW'              => "rectangle",
    'FUNCTION'          => "parallelogram",
    'PACKAGE'           => "parallelogram",
    'PROCEDURE'         => "parallelogram",
    'SEQUENCE'          => "ellipse",
    'DEFAULT'           => "rectangle",
);

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@EXPORT = qw(
    new_graph
    node_color
    node_shape
    get_nodes
    generate_graph
);

@ISA = qw(Exporter);

# TODO: prepare and extract user query as table
# TODO : use $sql = $dbh->quote_identifier( $name );

sub new_graph {
    my $this = shift;

    my %args = @_;

    my %aoi;
    if ( exists $args{config} ) {
        my $config   = $args{config};
        my @cfg_keys = $config->get_keys();
        $aoi{$_} = $config->get_value($_) for (@cfg_keys);
    }

    foreach my $key ( keys %args ) {
        $aoi{$key} = $args{$key};
    }

    my $format = $aoi{format} || Carp::croak("No format specified\n.");

    my $driver_class = "DataDict::Graph::Dependency::$format";
    eval qq{package                     # hide from PAUSE
        DataDict::Extractor::_firesafe; # just in case
        require $driver_class;          # load the driver
    };

    if ($@) {
        my $err = $@;
        Carp::croak("install_driver($format) failed: $err\n");
    }

    my $graph = $driver_class->_init(%aoi);

    return $graph;
}

sub _init {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    my %args = @_;
    $self->{$_} = $args{$_} for ( keys %args );

    #$self->{logger} ||= DataDict::Logger->new_logger(%args);

    unless ( $self->{objects} ) {
        $self->{extractor} ||= DataDict::Extractor->new_extractor( 'config' => $self->{config} );

        $self->{extractor}->set_schema( $self->{schema} );
        #$self->{logger}->log_info("Getting metadata for " . $self->{schema} . " ...");

        my $table_filter = $self->{config}->get_table_filter( $self->{schema} );
        my %objects;
        $objects{$_} = $self->{extractor}->get_objects( $_, $table_filter )
            for ( 'TABLE', 'PRIMARY KEY', 'COLUMN', 'DEPENDENCY', 'DEPENDENT' );

        $self->{objects} = \%objects;

        $self->{db_version}     ||= $self->{extractor}->get_db_version()     || '';
        $self->{db_comment}     ||= $self->{extractor}->get_db_comment()     || '';
        $self->{db_encoding}    ||= $self->{extractor}->get_db_encoding()    || '';
        $self->{schema_comment} ||= $self->{extractor}->get_schema_comment() || '';
    }

    $self->{date_time} = strftime "%Y-%m-%d %H:%M:%S", localtime;
    $self->_post_init();
    return $self;
}

sub _post_init { return undef }
sub graph      { return undef }
sub nodes      { return undef }

sub node_color {
    my ( $self, $type ) = @_;
    my $color = $type_colors{ uc $type } || $type_colors{DEFAULT};
    return $color;
}

sub node_shape {
    my ( $self, $type ) = @_;
    my $shape = $type_shapes{ uc $type } || $type_shapes{DEFAULT};
    return $shape;
}

sub node_types {
    my ($self) = @_;
    my @types = grep { $_ ne 'DEFAULT' } sort keys %type_shapes;
    return @types;
}

sub text_width {
    my ( $self, $font_family, $font_size, $font_style, $text ) = @_;
    my $text_width = 0;

    # TODO:
    if ( defined $text && $font_size ) {
        $text_width = ( $font_size - 3 ) * length($text);
    }

    return $text_width;
}

1;
