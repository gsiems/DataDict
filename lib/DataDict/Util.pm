package DataDict::Util;
use strict;
use warnings;

use File::Copy;
use File::Spec;

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@ISA = qw(Exporter);

my %done_dirs;

sub new_util {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    my @keys = (qw(logger log_file log_level append_log_file));
    my %args = @_;

    my %aoi;
    if ( exists $args{config} ) {
        my $config = $args{config};
        $aoi{$_} = $config->get_value($_) for (@keys);
    }

    foreach my $key (@keys) {
        if ( exists $args{$key} ) {
            $aoi{$key} = $args{$key};
        }
    }

    $self->{$_} = $args{$_} for ( keys %aoi );
    $self->{logger} ||= DataDict::Logger->new_logger(%aoi);

    return $self;
}

sub mkpath {
    my $self = shift;
    my ( $path, $mode ) = @_;
    return 1 if ( exists $done_dirs{$path} || -d $path );

    $mode = defined $mode ? $mode : 0777;
    $path = $self->abs_path($path);
    $self->{logger}->log_info("Creating $path directory");

    my $dir  = '/';
    my @dirs = File::Spec->splitdir($path);

    foreach (@dirs) {
        next unless ($_);
        $dir = File::Spec->catdir( $dir, $_ );
        if ( -d $dir ) {
            $done_dirs{$path} = 1;
        }
        elsif ( mkdir( $dir, $mode ) ) {
            $done_dirs{$path} = 1;
        }
        else {
            $self->_log_fatal("mkdir($dir) failed: $!");
        }
    }
    return ( -d $path );
}

sub _log_fatal {
    my ( $self, @messages ) = @_;
    if ( $self->{logger} ) {
        $self->{logger}->log_fatal(@messages);
    }
    die "Utility failure.\n";
}

sub init_html_destination {
    my ( $self, $source_base, $target_base ) = @_;

    $source_base = $self->abs_path($source_base);
    $target_base = $self->abs_path($target_base);

    foreach my $dirname (qw(css img js)) {
        $self->{logger}->log_info("Initializing $dirname directory");

        my $source_path = File::Spec->catdir( $source_base, $dirname );
        my $target_path = File::Spec->catdir( $target_base, $dirname );

        $self->mkpath($target_path) unless ( -d $target_path );
        next unless ( -d $source_path && -d $target_path );

        $self->{logger}->log_info("Populating $target_path directory");

        my $DIR;
        if ( opendir $DIR, $source_path ) {
            my @dir = readdir $DIR;
            closedir $DIR;
            foreach my $file (@dir) {
                my $source = File::Spec->catfile( $source_path, $file );
                my $target = File::Spec->catfile( $target_path, $file );
                next unless ( -f $source );
                copy( $source, $target );
            }
        }
    }
}

sub dir_path {
    my ( $self, @ary ) = @_;
    my ( undef, $dir, undef ) = File::Spec->splitpath( $self->abs_path(@ary) );
    return $dir;
}

sub abs_path {
    my ( $self, @ary ) = @_;
    my $path = $self->cat_path(@ary);
    $path = File::Spec->rel2abs($path) unless ( File::Spec->file_name_is_absolute($path) );
    return $path;
}

sub rel_path {
    my ( $self, @ary ) = @_;
    my $path = $self->cat_path(@ary);
    $path = File::Spec->abs2rel($path) if ( File::Spec->file_name_is_absolute($path) );
    return $path;
}

sub cat_path {
    my ( $self, @ary ) = @_;
    my @path_segments;
    foreach my $segment (@ary) {
        push @path_segments, $_ for ( split '/', $segment );
    }
    my $path = File::Spec->catfile(@path_segments);
    return $path;
}

sub abs_cat_path {
    my ( $self, @ary ) = @_;
    return $self->abs_path( $self->cat_path(@ary) );
}

sub rel_cat_path {
    my ( $self, @ary ) = @_;
    return $self->rel_path( $self->cat_path(@ary) );
}

1;
__END__
