package DataDict::Writer;
use strict;
use warnings;

use IO::File;
#use Compress::Zlib;
use POSIX qw(locale_h);
setlocale( LC_NUMERIC, "C" );

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@EXPORT = qw(
    new_writer
    write
    close
);

@ISA = qw(Exporter);

my $logger;

sub new_writer {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    # set initial attributes
    my %args = @_;
    $self->{$_} = $args{$_} for ( keys %args );

    if ( exists $self->{logger} ) {
        $logger = $self->{logger};
    }

    $self->{append} ||= 0;              # default is to over-write
    $self->{compression_level} ||= 4;

    # mode ?

    if ( $self->{file} && $self->{file} =~ m/gz$/i ) {
        $self->_open_gzip_output();
    }
    elsif ( $self->{file} ) {
        $self->_open_text_output();
    }
    #    else {
    #        $self->_log_fatal ("No output file specified");
    #    }

    return $self;
}

sub _open_text_output {
    my ($self) = @_;
    my $mode = ( $self->{append} ) ? '>>' : '>';
    $self->{type}   = 'text';
    $self->{handle} = new IO::File;
    $self->{handle}->open( $self->{file}, $mode )
        || $self->_log_fatal( "Could not open " . $self->{file} . " for output. $!" );
    $self->{handle}->binmode();
}

sub _open_gzip_output {
    my ($self) = @_;
    my $mode = ( $self->{append} ) ? 'ab' : 'wb';
    $mode .= $self->{compression_level};
    $self->{type} = 'gz';
    use Compress::Zlib;
    $self->{handle} = gzopen( $self->{file}, $mode )
        || $self->_log_fatal( "Could not open " . $self->{file} . " for output. $gzerrno" );
}

sub write {
    my ( $self, $data ) = @_;

    if ( $self->{handle} && $self->{type} eq 'gz' ) {
        $self->{handle}->gzwrite($data) || $self->_log_fatal( "Could not write to " . $self->{file} . ". $gzerrno" );
    }
    elsif ( $self->{handle} ) {
        $self->{handle}->print($data) || $self->_log_fatal( "Could not write to " . $self->{file} . ". $!" );
    }
    else {
        print $data;
    }
}

sub close {
    my ($self) = @_;

    if ( $self->{handle} ) {
        if ( $self->{type} eq 'gz' ) {
            $self->{handle}->gzclose;
        }
        else {
            $self->{handle}->close;
        }
    }
    undef $self->{handle};
}

sub _log_fatal {
    my ( $self, @messages ) = @_;
    if ($logger) {
        $logger->log_fatal(@messages);
    }
    else {
        foreach my $message (@messages) {
            print STDERR "FATAL: $_ \n";
        }
    }
    die "Write failed.\n";
}

1;
__END__
