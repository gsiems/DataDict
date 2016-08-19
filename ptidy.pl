#!/usr/bin/env perl
use warnings;
use strict;

use Perl::Critic;
use FindBin;

my $critic = Perl::Critic->new();

my $lib_path = "$FindBin::Bin/lib";

my @files = `find $lib_path -type f -name "*.pm"`;
chomp @files;

foreach my $file (@files) {
    `perltidy -pro=./perltidyrc $file`;
    my @diff = `diff $file $file.tdy`;
    if (@diff) {
        print "$file and $file.tdy differ\n";
    }
    else {
        unlink "$file.tdy";
    }
}
