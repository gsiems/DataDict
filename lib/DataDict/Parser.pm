package DataDict::Parser;
use strict;
use warnings;
use DBI;
use Carp();
use Data::Dumper;

our $placeholder_idx = 0;

sub new_parser {
    my $this = shift;

    my $class = ref($this) || $this;
    my $self = {};
    bless $self, $class;

    $self->{item_sep_re} = q{,};
    $self->{double_quoted_re} =
        q{(?<!\\\)["]};    # Negative look behind: '"' but not '\"'  -- double quotes in identifiers
    $self->{single_quoted_re}   = q{[']+};
    $self->{parens_re}          = q{[()]};
    $self->{cmt_line_start_re}  = q{\-\-};
    $self->{cmt_line_end_re}    = q{\n};
    $self->{cmt_block_start_re} = q{/\*};
    $self->{cmt_block_end_re}   = q{\*\/};
    $self->{statement_end_re}   = q{[;]};

    return $self;
}

sub convert_decode {
    my ( $self, $string ) = @_;

    $string =~ s/\n/ /g;

    my $re = '(' . join( '|', $self->{item_sep_re}, $self->{parens_re} ) . ')';

    my @tokens = split /$re/, $string;

    my $parens_count = 0;
    my @chk_pts;
    my @comma_count = (0);
    my @ary;

    foreach my $token (@tokens) {
        if ( $token =~ m/\b(DECODE)\b/i ) {
            push @chk_pts, $parens_count;
            if ( @ary && $comma_count[-1] % 2 == 1 ) {    # Nested DECODE statements
                $ary[-1] =~ s/^WHEN /ELSE /;
            }
            push @comma_count, 0;

            $token =~ s/DECODE\s*/CASE /i;
            push @ary, $token;
            next;
        }
        elsif (@chk_pts) {
            if ( $token eq '(' ) {
                if ( $chk_pts[-1] != $parens_count ) {
                    $ary[-1] .= $token;
                }
                $parens_count++;
                next;
            }
            elsif ( $token eq ')' ) {
                $parens_count--;
                if ( $parens_count == $chk_pts[-1] ) {
                    if ( @ary && $comma_count[-1] % 2 == 1 ) {
                        $ary[-1] =~ s/^WHEN /ELSE /;
                    }
                    push @ary, 'END ';
                    pop @chk_pts;
                    pop @comma_count;
                    next;
                }
            }
            elsif ( $token eq ',' ) {
                if ( $chk_pts[-1] + 1 == $parens_count ) {
                    if ( $comma_count[-1] % 2 ) {
                        $ary[-1] .= ' THEN ';
                    }
                    else {
                        push @ary, 'WHEN ';
                    }
                    $comma_count[-1]++;
                    next;
                }
            }
        }
        push @ary, '' unless (@ary);
        $ary[-1] .= $token if ($token);
    }

    for (@ary) {
        s/[\s\n]+$//;
        s/^[\s\n]+//;
        s/\s\s+/ /g;
    }

    @ary = grep { $_ !~ m/^\s*$/ } @ary;

    return +( join "\n", @ary );
}

sub extract_function {
    my ( $self, $fcn_name, $string ) = @_;

    my ( $return, $fcn, $remainder ) = split /\b($fcn_name)\b/is, $string, 2;

    $return    ||= '';
    $fcn       ||= '';
    $remainder ||= '';

    my $fcn_placeholder = 'placeholder_fcn_' . sprintf( "%06d", length($return) );
    my $parens_count    = 1;
    my $in_fcn          = 1;

    my ( $fore, $parens, $aft ) = split /(\s*[(])/is, $remainder, 2;
    $fcn .= "$fore$parens";

    my @ary = split /([()])/, $aft;
    foreach my $token (@ary) {
        if ( $token eq '(' ) {
            $parens_count++;
        }
        elsif ( $token eq ')' ) {
            $parens_count--;
        }
        if ($in_fcn) {
            $fcn .= $token;
            if ( $parens_count == 0 ) {
                $in_fcn = 0;
                $return .= $fcn_placeholder;
            }
        }
        else {
            $return .= $token;
        }
    }

    return ( $fcn_placeholder, $fcn, $return );
}

sub fix_capitalization {
    my ( $self, %args ) = @_;
    my @sql = @{ $args{sql} };

    my $keywords_case = 'upper';
    if ( $args{keywords_case} ) {
        $keywords_case = lc $args{keywords_case};
    }

    my $non_keywords_case = 'lower';
    if ( $args{non_keywords_case} ) {
        $non_keywords_case = lc $args{non_keywords_case};
    }

    my %keywords = $self->_keywords();
    foreach my $string (@sql) {
        my @tokens = split /\b/, $string;
        foreach my $idx ( 0 .. $#tokens ) {
            my $token      = $tokens[$idx];
            my $is_keyword = 0;
            if ( exists $keywords{ uc $token } ) {
                unless ( ( $idx && $tokens[ $idx - 1 ] eq '.' )
                    || ( $idx < $#tokens && $tokens[ $idx + 1 ] eq '.' ) )
                {
                    $is_keyword = 1;
                }
            }
            if ($is_keyword) {
                if ( $keywords_case eq 'upper' ) {
                    $token = uc $token;
                }
                elsif ( $keywords_case eq 'lower' ) {
                    $token = lc $token;
                }
                $tokens[$idx] = $token;
            }
            else {
                if ( $non_keywords_case eq 'upper' ) {
                    $token = uc $token;
                }
                elsif ( $non_keywords_case eq 'lower' ) {
                    $token = lc $token;
                }
                $tokens[$idx] = $token;
            }
        }
        $string = join( '', @tokens );
    }
    return @sql;
}

sub fix_indentation {
    my ( $self, %args ) = @_;
    my @sql = @{ $args{sql} };
    my @return;
    my $base_indent = $args{base_indent} || 0;

    my $indent_char = ' ';
    if ( $args{indent_char} && ( uc $args{indent_char} eq 'TAB' || $args{indent_char} eq "\t" ) ) {
        $indent_char = "\t";
    }

    my $indent_size;
    if ( $args{indent_size} ) {
        $indent_size = $args{indent_size};
    }
    elsif ( $indent_char eq "\t" ) {
        $indent_size = 1;
    }
    else {
        $indent_size = 4;
    }

    my $pre_parens_count  = 0;
    my $post_parens_count = 0;
    my $current_indent    = 0;
    my $init_offset       = 0;
    my $fcn_offset        = 0;
    my $current_clause    = '';
    my @fcn_clause        = ('');

    foreach my $line (@sql) {
        $line =~ s/^\s+//;
        $line =~ s/\s+$//;

        my $open_parens_count  = () = $line =~ m/(\()/g;
        my $close_parens_count = () = $line =~ m/(\))/g;
        $post_parens_count += ( $open_parens_count - $close_parens_count );

        if ( $line =~ m/^(UNION|MINUS|EXCEPT|INTERSECT)\b/i && $line !~ m/^(SELECT)\b/i ) {
            $current_clause = '';
            $init_offset    = -2;
        }
        elsif ( $line =~ m/^(UNION|MINUS|EXCEPT|INTERSECT)\b/i && $line =~ m/(SELECT)\b/i ) {
            $current_clause = 'SELECT';
            $init_offset    = -2;
        }
        elsif ( $line =~ m/^(SELECT|WITH)\b/i ) {
            $current_clause = uc $1;
            $init_offset    = -2;
        }
        elsif ( $line =~ m/^(RIGHT|LEFT|FULL)*\s*(INNER|OUTER)*\s*(JOIN)\b/i ) {
            $current_clause = 'JOIN';
            $init_offset    = -1;
        }
        elsif ( $line =~ m/^(FROM|WITH|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|OFFSET)\b/i ) {
            $current_clause = uc $1;
            $init_offset    = -1;
        }
        elsif ( $line =~ m/^(CREATE|DROP|ALTER|GRANT|REVOKE|ADD|COMMENT)\b/i ) {
            $current_clause = uc $1;
            $init_offset    = -2;
        }

        $current_indent = ( $pre_parens_count * 2 ) + 2 + $init_offset + $fcn_offset + $base_indent;
        $current_indent -= 2 if ( $line =~ m/^\s*\)/ );

        my $leading_spaces = $indent_char x ( $indent_size * $current_indent );
        $line = $leading_spaces . $line;
        push @return, $line;

        if ( $line =~ m/^\s*(CASE)\b.+\b(WHEN)\b/i ) {
            my @ary = split /\b(WHEN|ELSE|END)\b/i, $line;
            $return[-1] = shift @ary;
            foreach my $token (@ary) {
                if ( $token =~ m/\b(WHEN|ELSE|END)\b/i ) {
                    push @return, $leading_spaces . ( $indent_char x $indent_size ) . $token;
                }
                else {
                    $return[-1] .= $token;
                }
            }
        }
        elsif ( $line =~ m/^\s*(CASE)\b/i ) {
            push @fcn_clause, 'CASE';
            $fcn_offset++;
        }
        elsif ( $line =~ m/^\s*(END)\b/i && $fcn_clause[-1] eq 'CASE' ) {
            pop @fcn_clause;
            $fcn_offset--;
        }

        $init_offset      = 0;
        $pre_parens_count = $post_parens_count;
    }

    return @return;
}

sub fix_wrapping {
    my ( $self, %args ) = @_;
    my @sql = @{ $args{sql} };
    my $base_indent = $args{base_indent} || 0;

    my $indent_char = ' ';
    if ( $args{indent_char} && ( uc $args{indent_char} eq 'TAB' || $args{indent_char} eq "\t" ) ) {
        $indent_char = "\t";
    }

    my $indent_size;
    if ( $args{indent_size} ) {
        $indent_size = $args{indent_size};
    }
    elsif ( $indent_char eq "\t" ) {
        $indent_size = 1;
    }
    else {
        $indent_size = 4;
    }

    my $max_line_length = 132;
    if ( $args{max_line_length} ) {
        $max_line_length = $args{max_line_length};
    }

    my $placeholders = $args{placeholders};

    my @wrapped;

    foreach my $line (@sql) {
        my ($indent) = $line =~ m/^(\s*)/;
        my $new_line = '';
        my @tokens = split /([\s(),]+)/, $line;
        my @ary;
        my $line_len = 0;
        foreach my $token (@tokens) {

            if ( $token =~ m/\s+/ ) {
                $new_line .= $token;
                $line_len += length($token);
            }
            elsif ( exists $placeholders->{$token} ) {
                if ( $new_line !~ m/^\s*$/ && ( $line_len + length( $placeholders->{$token} ) ) > $max_line_length ) {
                    push @ary, $new_line;
                    $new_line = $indent . ( $indent_char x $indent_size ) . $token;
                    $line_len = length($new_line);
                }
                else {
                    $new_line .= $token;
                    $line_len += length($token);
                }
            }
            else {
                if ( $new_line !~ m/^\s*$/ && ( $line_len + length($token) ) > $max_line_length ) {
                    push @ary, $new_line;
                    $new_line = $indent . ( $indent_char x $indent_size ) . $token;
                    $line_len = length($new_line);
                }
                else {
                    $new_line .= $token;
                    $line_len += length($token);
                }
            }
        }
        push @ary, $new_line unless ( $new_line =~ m/^\s*$/ );
        push @wrapped, $_ for (@ary);
    }
    return @wrapped;
}

sub parse_statement {
    my ( $self, %args ) = @_;

    my $sql = $args{sql};
    my $default_case = $args{default_case} || 'upper';

    # substitute oracle outer-joins "/\(\s*\+\s*\)/" with placeholders
    {
        my $key = 'placeholder_ooj';
        $self->{placeholders}{$key} = '(+)';
        $sql =~ s/\(\s*\+\s*\)/$key/g;
    }

    # substitute empty parens "/\(\s*\)/" with placeholders
    {
        my $key = 'placeholder_ep';
        $self->{placeholders}{$key} = '()';
        $sql =~ s/\(\s+\)/$key/g;
    }

    $sql = $self->_extract_strings( 'sql' => $sql, 'default_case' => $default_case );
    $sql = $self->_extract_comments( 'sql' => $sql );

    my @clauses = $self->_split_clauses($sql);
    @clauses = $self->_split_tab_col_clauses(@clauses);
    @clauses = $self->_split_where_clauses(@clauses);
    @clauses = $self->_split_odd_parens(@clauses);

    $_ =~ s/^\s+// for (@clauses);
    my @parsed = grep { defined $_ && $_ !~ m/^\s*$/ } @clauses;

    # fix/unify whitespace
    foreach (@parsed) {
        s/([()])/ $1 /g;
        s/\s+;/;/g;
        s/\s*,\s*/, /g;
        s/^\s+//;
        s/\s*$//;
        s/\s\s+/ /g;
    }

    return ( \@parsed, $self->{placeholders} );
}

sub _extract_strings {
    my ( $self, %args ) = @_;

    my $sql          = $args{sql};
    my $default_case = $args{default_case} || 'upper';
    my $new_sql      = '';
    my $find         = '';
    my $do_replace   = 0;

    # Extract strings and quoted identifiers
    my $re = '('
        . join( '|',
        $self->{double_quoted_re},   $self->{single_quoted_re},
        $self->{cmt_line_start_re},  $self->{cmt_line_end_re},
        $self->{cmt_block_start_re}, $self->{cmt_block_end_re} )
        . ')';

    my ( $single_quoted, $double_quoted, $cmt_line, $cmt_block ) = ( 0, 0, 0, 0 );

    my @tokens = split /$re/i, $sql;
    foreach my $token (@tokens) {
        if ( $token =~ m/^'/ ) {
            $find .= $token;
            unless ( $double_quoted || $cmt_line || $cmt_block ) {
                if ( ( length $token ) % 2 ) {    # "'", "'''", ... quotes at begining or end of quotes string
                    $do_replace = 1 if ($single_quoted);
                    $single_quoted = !$single_quoted;
                }
                elsif ( !$single_quoted ) {
                    $do_replace = 1;
                }
            }
        }
        elsif ( $token eq '"' ) {
            $find .= $token;
            unless ( $single_quoted || $cmt_line || $cmt_block ) {
                $do_replace = 1 if ($double_quoted);
                $double_quoted = !$double_quoted;
            }
        }
        elsif ( $token eq '/*' ) {
            $find .= $token;
            $cmt_block = 1 unless ( $single_quoted || $double_quoted || $cmt_line || $cmt_block );
        }
        elsif ( $token eq '*/' ) {
            $find .= $token;
            $do_replace = 1 if ($cmt_block);
            $cmt_block = 0;
        }
        elsif ( $token eq '--' ) {
            $find .= $token;
            $cmt_line = 1 unless ( $single_quoted || $double_quoted || $cmt_line || $cmt_block );
        }
        elsif ( $token eq "\n" ) {
            if ( $single_quoted || $double_quoted || $cmt_block ) {
                $find .= $token;
            }
            elsif ($cmt_line) {
                $find .= $token;
                $do_replace = 1;
                $cmt_line   = 0;
            }
            else {
                $new_sql .= $token;
            }
        }
        elsif ( $single_quoted || $double_quoted || $cmt_line || $cmt_block ) {
            $find .= $token;
        }
        else {
            $new_sql .= $token;
        }

        if ($do_replace) {
            # ASSERT: quote_ident is '"' and that quoting identifiers is optional...
            if ( $find =~ m/^"/ ) {
                if ( $default_case eq 'upper' && $find eq uc $find && $find !~ m/(\\|\s|\n|\r)+/ ) {
                    $find =~ s/"//g;
                    $new_sql .= $find;
                }
                elsif ( $default_case eq 'lower' && $_ eq lc $_ && $_ !~ m/(\\|\s)+/ ) {
                    $find =~ s/"//g;
                    $new_sql .= $find;
                }
                else {
                    $placeholder_idx++;
                    my $key = 'placeholder_ident_' . sprintf( "%06d", $placeholder_idx );
                    $self->{placeholders}{$key} = $find;
                    $new_sql .= $key;
                }
            }
            elsif ( $find =~ m/^(')/ ) {
                $placeholder_idx++;
                my $key = 'placeholder_str_' . sprintf( "%06d", $placeholder_idx );
                $self->{placeholders}{$key} = $find;
                $new_sql .= $key;
            }
            else {
                $new_sql .= $find;
            }
            $find       = '';
            $do_replace = 0;
        }
    }

    return $new_sql;
}

sub _extract_comments {
    my ( $self, %args ) = @_;

    my $sql        = $args{sql};
    my $new_sql    = '';
    my $find       = '';
    my $do_replace = 0;

    my $re = '('
        . join( '|',
        $self->{cmt_line_start_re},  $self->{cmt_line_end_re},
        $self->{cmt_block_start_re}, $self->{cmt_block_end_re} )
        . ')';

    my ( $cmt_line, $cmt_block ) = ( 0, 0 );

    my @tokens = split /$re/i, $sql;
    foreach my $token (@tokens) {
        if ( $token eq '/*' ) {
            $find .= $token;
            $cmt_block = 1 unless ( $cmt_line || $cmt_block );
        }
        elsif ( $token eq '*/' ) {
            $find .= $token;
            $do_replace = 1 if ($cmt_block);
            $cmt_block = 0;
        }
        elsif ( $token eq '--' ) {
            $find .= $token;
            $cmt_line = 1 unless ( $cmt_line || $cmt_block );
        }
        elsif ( $token eq "\n" ) {
            if ($cmt_block) {
                $find .= $token;
            }
            elsif ($cmt_line) {
                $find .= $token;
                $do_replace = 1;
                $cmt_line   = 0;
            }
            else {
                $new_sql .= $token;
            }
        }
        elsif ( $cmt_line || $cmt_block ) {
            $find .= $token;
        }
        else {
            $new_sql .= $token;
        }

        if ($do_replace) {
            $placeholder_idx++;
            my $key = 'placeholder_cmt_' . sprintf( "%06d", $placeholder_idx );

            if ( $new_sql =~ m/\n\s*$/ ) {
                $find = "\n" . $find;
            }

            $self->{placeholders}{$key} = $find;
            $new_sql .= $key . ' ';    # ???

            $find       = '';
            $do_replace = 0;
        }
    }

    return $new_sql;
}

sub _split_clauses {
    my ( $self, $sql ) = @_;

    my @return = ('');
    my $crw    = q{SELECT
    UNION
    EXCEPT
    MINUS
    INTERSECT
    FROM
    RIGHT OUTER JOIN
    RIGHT JOIN
    LEFT OUTER JOIN
    LEFT JOIN
    FULL OUTER JOIN
    FULL JOIN
    INNER JOIN
    JOIN
    WITH
    WHERE
    GROUP BY
    ORDER BY
    HAVING
    INSERT
    VALUES
    UPDATE
    DELETE
    TRUNCATE
    EXECUTE
    GRANT
    REVOKE
    CREATE
    ALTER
    DROP
    ROLLBACK
    COMMIT
    ON
    USING
    ADD CONSTRAINT
    DROP CONSTRAINT
    CONSTRAINT};

    $crw =~ s/\s*\n\s*/\\b|\\b/g;
    $crw =~ s/\s+/\\s+/g;
    my $return_re = '\b' . $crw . '\b';
    my $re        = '([;]|' . $return_re . ')';

    foreach my $token ( split /$re/i, $sql ) {

        if ( $token =~ m/[;]/ ) {
            $return[-1] .= $token;
            push @return, '';
        }
        elsif ( $token =~ m/$return_re/i ) {
            my ( $before, $kw, $after ) = split /($return_re)/i, $token;
            $return[-1] .= $before;

            if ( $return[-1] =~ m/WITHIN GROUP\s*\(\s*$/i ) {    # Oracle's "listagg () within group (order by ...)"
                $return[-1] .= "$kw$after";
            }
            else {
                push @return, "$kw$after";
            }
        }
        else {
            $return[-1] .= $token;
        }
    }

    return @return;
}

sub _split_tab_col_clauses {
    my ( $self, @clauses ) = @_;

    my @return = ('');
    foreach my $clause (@clauses) {
        foreach my $token ( split /([,])/, $clause ) {
            $return[-1] .= $token;
            if ( $token eq ',' ) {
                my $line_op_count = () = $return[-1] =~ m/(\()/g;
                my $line_cp_count = () = $return[-1] =~ m/(\))/g;
                if ( $line_op_count <= $line_cp_count ) {
                    push @return, '';
                }
            }
        }
        push @return, '';
    }
    return @return;
}

sub _split_where_clauses {
    my ( $self, @clauses ) = @_;

    my @return = ('');
    foreach my $clause (@clauses) {
        if ( $clause =~ m/^\s*(WHERE)\b/i ) {
            # Wrap the ANDs and ORs
            my @ary = split /\b(AND|OR)\b/i, $clause;
            push @return, shift @ary;

            foreach my $token (@ary) {
                if ( $token =~ m/\b(AND|OR)\b/i ) {
                    push @return, $token;
                }
                else {
                    $return[-1] .= $token;
                }
            }
        }
        else {
            push @return, $clause;
        }
    }
    return @return;
}

sub _split_odd_parens {
    my ( $self, @clauses ) = @_;

    my @return;
    foreach my $line (@clauses) {
        my $line_op_count = () = $line =~ m/(\()/g;
        my $line_cp_count = () = $line =~ m/(\))/g;
        if ( $line_op_count == $line_cp_count ) {
            push @return, $line;
            next;
        }

        push @return, '';
        my $op_count = 0;
        my $cp_count = 0;
        foreach my $token ( split /([()])/, $line ) {
            if ( $token eq '(' ) {
                if ( ( $line_op_count - $op_count ) > $line_cp_count ) {
                    push @return, '';
                }
                $op_count++;
            }
            elsif ( $token eq ')' ) {
                $cp_count++;
                if ( $cp_count > $line_op_count ) {
                    push @return, '';
                }
            }
            $return[-1] .= $token;
        }
    }
    return @return;
}

sub _keywords {
    my ($self) = @_;
    my %keywords = map { $_ => 1 } (
        qw(
            ALTER
            DROP
            ADD
            PRIMARY KEY
            FOREIGN KEY
            REFERENCES
            INDEX
            TABLESPACE
            COMMENT
            COLUMN
            TABLE
            ALL
            AND
            ANY
            AS
            ASC
            AUTOMATIC
            BETWEEN
            BLOCK
            BREADTH
            BY
            CASE
            CHECK
            COMMIT
            CONNECT
            CONSTRAINT
            CROSS
            CUBE
            CURRENT
            CYCLE
            DECREMENT
            DEFAULT
            DELETE
            DEPTH
            DESC
            DIMENSION
            DISTINCT
            ELSE
            END
            EXCEPT
            EXECUTE
            EXCLUDE
            FETCH
            FIRST
            FOR
            FROM
            FULL
            GROUP
            GRANT
            REVOKE
            GROUPING
            HAVING
            IMMEDIATE
            IN
            INCLUDE
            INCREMENT
            INNER
            INSERT
            INTERSECT
            INTO
            IS
            ITERATE
            JOIN
            LAST
            LEFT
            LIKE
            LIMIT
            LOCKED
            MAIN
            MAXVALUE
            MEASURES
            MERGE
            MINUS
            MINVALUE
            MODEL
            NATURAL
            NEXT
            NOCYCLE
            NOT
            NOWAIT
            NULL
            NULLS
            OF
            OFFSET
            ON
            ONLY
            OPTION
            ORDER
            OUTER
            PARTITION
            PIVOT
            READ
            RECURSIVE
            REFERENCE
            RETURN
            RETURNING
            RIGHT
            ROLLUP
            ROW
            ROWS
            RULES
            SAMPLE
            SCN
            SEARCH
            SEED
            SELECT
            SEQUENTIAL
            SET
            SETS
            SHARE
            SIBLINGS
            SKIP
            START
            SUBPARTITION
            TABLE
            TEMP
            TEMPORARY
            THEN
            TIMESTAMP
            TO
            UNION
            UNIQUE
            UNLOGGED
            UNPIVOT
            UNTIL
            UPDATE
            UPDATED
            UPSERT
            USING
            VALUES
            VERSIONS
            WAIT
            WHERE
            WHEN
            WINDOW
            WITH
            WITHIN
            XML
            CREATE
            OR
            REPLACE
            PROCEDURE
            FUNCTION
            FORCE
            VIEW
            )
    );

    return %keywords;
}

sub _log_fatal {
    my ( $self, @messages ) = @_;
    if ( $self->{logger} ) {
        $self->{logger}->log_fatal(@messages);
    }
    die "Extract failed.\n";
}

1;
__END__

