#!/bin/sh

# Purpose: To "compile" the template files in order to minimize the
# space of the output files. In reality this amounts to simply removing
# the extra white-space as doing so can reduce the size of the data
# dictionaries by 30 or more percent.

for file in `find src -type f` ; do
    newfile=`echo $file | sed 's/src\///'`
    cat $file | perl -pe 's/^\s+//g' | tr -d "\n" > $newfile
done
