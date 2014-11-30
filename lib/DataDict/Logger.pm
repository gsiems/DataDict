package DataDict::Logger;

=head1 NAME

DataDict::Logger

=head1 SYNOPSIS

Message logging module for the DataDict tools.

=cut

use strict;
use warnings;

use POSIX qw(strftime);
use Carp();

use Exporter;
use vars qw($VERSION @EXPORT @ISA);
$VERSION = '0.1';

@EXPORT = qw(
    new_logger
    log_level
    log_fatal
    log_error
    log_warning
    log_info
    log_debug
);

@ISA = qw(Exporter);

my $logfh;

my %LOG_LEVELS = (
    OFF     => 0,
    FATAL   => 1,
    ERROR   => 2,
    WARNING => 3,
    INFO    => 4,
    DEBUG   => 5,
);

my %LOG_FLAGS = (
    0 => 'OFF',
    1 => 'FATAL',
    2 => 'ERROR',
    3 => 'WARNING',
    4 => 'INFO',
    5 => 'DEBUG',
);

=head2 new_logger

Creates a new logger instance and returns a handle to the logger.

Calling parameters:

=head3 log_level (OFF|FATAL|ERROR|WARNING|INFO|DEBUG)

Controls which message levels are written to the log.

Valid values are OFF, FATAL, ERROR, WARNING, INFO, and DEBUG.
The lower the level, the fewer messages are sent to the log.
With the exception of OFF, each level includes all lower levels (eg.,
setting the level to WARNING results in all WARNING, ERROR and FATAL
messages being logged).

Defaults to WARNING if no level is specified.

=head3 log_file <file_name>

The file name to log to. Defaults to STDERR if no log file is specified.

=head3 append_log_file   (0|1)

Whether to append to the log file (if it already exists) or to
over-write it. Defaults to over-writing if not specified.

=cut

sub new_logger {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $self  = {};
    bless $self, $class;

    # set initial attributes
    my %args = @_;

    my $log_level = ( exists $args{log_level} && $args{log_level} ) ? $LOG_LEVELS{ $args{log_level} } : undef;
    $self->{log_level}   = $log_level             || $LOG_LEVELS{WARNING};
    $self->{append_file} = $args{append_log_file} || 0;                      # default is to over-write
    $self->{log_file}    = $args{log_file}        || undef;

    if ( $self->{log_file} ) {
        my $mode = ( $self->{append_file} ) ? '>>' : '>';
        open( $logfh, $mode, $self->{log_file} )
            || Carp::croak( "Could not open log file (" . $self->{log_file} . ") for output. $!\n" );
    }

    return $self;
}

=head2 log_level ([new_level])

Set or retrieve the logging level.

    OFF       Turns off logging.
    FATAL     Reports errors that caused the application to abort.
    ERROR     Reports errors that caused the current operation to abort.
    WARNING   Reports warnings of problems with the current operation.
    INFO      Reports general information such as application progress.
    DEBUG     Reports more detailed information for use by developers.

=cut

sub log_level {
    my ( $self, $new_level ) = @_;
    if ( defined $new_level && exists $LOG_LEVELS{$new_level} ) {
        $self->{log_level} = $LOG_LEVELS{$new_level};
    }
    return $LOG_FLAGS{ $self->{log_level} };
}

=head2 log_fatal (@messages)

Logs one or more FATAL level messages. It is asserted that the calling
code will terminate the application in the event of a fatal error.

=cut

sub log_fatal {
    my ( $self, @messages ) = @_;
    $self->_write_log( 'FATAL', @messages );
}

=head2 log_error (@messages)

Logs one or more ERROR level messages.

=cut

sub log_error {
    my ( $self, @messages ) = @_;
    $self->_write_log( 'ERROR', @messages );
}

=head2 log_warning (@messages)

Logs one or more WARNING level messages.

=cut

sub log_warning {
    my ( $self, @messages ) = @_;
    $self->_write_log( 'WARNING', @messages );
}

=head2 log_info (@messages)

Logs one or more INFO level messages.

=cut

sub log_info {
    my ( $self, @messages ) = @_;
    $self->_write_log( 'INFO', @messages );
}

=head2 log_debug (@messages)

Logs one or more DEBUG level messages.

=cut

sub log_debug {
    my ( $self, @messages ) = @_;
    $self->_write_log( 'DEBUG', @messages );
}

sub _write_log {
    my ( $self, $flag, @messages ) = @_;
    my $level = $LOG_LEVELS{$flag};

    if ( $self->{log_level} > 0 && $level <= $self->{log_level} ) {
        my $tmsp = strftime "%Y-%m-%d %H:%M:%S", localtime;

        foreach my $message (@messages) {
            my $log_line = "$tmsp $flag: $message\n\n";

            if ($logfh) {
                print $logfh $log_line;
            }
            else {
                print STDERR $log_line;
            }
        }
    }
}

sub DESTROY {
    my ($self) = @_;
    #    print STDERR "DESTROY called by " . join ( ", ", caller) . "\n";
    if ($logfh) {
        close $logfh;
    }
}

1;
__END__
