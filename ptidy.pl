#!/usr/bin/env perl
use warnings;
use strict;

use Perl::Critic;
use FindBin;

my $critic = Perl::Critic->new();

my $path = "$FindBin::Bin";

my @files = `find $path -type f -name "*.p?" | grep -v git_ignore`;
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
