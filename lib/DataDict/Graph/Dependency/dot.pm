package DataDict::Graph::Dependency::dot;

use base 'DataDict::Graph::Dependency';

use strict;
use warnings;

sub graph {
    my ($self) = @_;
    my %nodes = $self->_graph_data();

    my $schema_name = $self->{schema};

    my $output = '';

    # TODO: what does/should the dot look like for this?
    return $output;

    my $y_sep = 20;

    my $id     = 0;
    my $legend = '';
    ( $legend, $id ) = $self->_legend( $id, $y_sep );
    $output .= $legend;

    $id++;
    my $title_block = '';
    ( $title_block, $id ) = $self->_title_block($id);
    $output .= $title_block;

    # Determine "other schema" groups, their ID's, height, etc.
    my %groups;
    my $x  = 0;
    my $y  = 0;
    my $x0 = 400;
    my $y0 = 300;

    foreach my $schema ( sort keys %nodes ) {

        foreach my $name ( sort keys %{ $nodes{$schema} } ) {
            $nodes{$schema}{$name}{height} = 70
                unless ( $nodes{$schema}{$name}{height} && $nodes{$schema}{$name}{height} > 70 );
            $nodes{$schema}{$name}{width} = 191.94140817732347
                unless ( $nodes{$schema}{$name}{width} && $nodes{$schema}{$name}{width} > 191.94140817732347 );
        }

        next if ( $schema eq $schema_name );
        my $total_height = 0;
        my $max_width    = 224.47070408866173;

        foreach my $name ( sort keys %{ $nodes{$schema} } ) {
            $total_height += $nodes{$schema}{$name}{height} + $y_sep;
            $max_width = $nodes{$schema}{$name}{width} if ( $max_width < $nodes{$schema}{$name}{width} );
        }
        $id++;

        $y = $y0 + $total_height / 2;
        $x = $x0 + $max_width / 2;

        $groups{$schema}{id}     = $id;
        $groups{$schema}{height} = $total_height;
        $groups{$schema}{width}  = $max_width;
        $groups{$schema}{x}      = $x;
        $groups{$schema}{x0}     = $x0;
        $groups{$schema}{y}      = $y;
        $groups{$schema}{y0}     = $y0;

        $y0 += $total_height + 3 * $y_sep;

        my $lgroup = << "EOT";
	node
	[
		id	$id
		label	"$schema"
		graphics
		[
			x	$x
			y	$y
			w	$max_width
			h	$total_height
			type	"roundrectangle"
			fill	"#CCFFCC"
			outline	"#666699"
			outlineStyle	"dotted"
			topBorderInset	0.0
			bottomBorderInset	13.853386622378821
			leftBorderInset	2.529295911338295
			rightBorderInset	0.0
		]
		LabelGraphics
		[
			text	"$schema"
			fill	"#99CCFF"
			fontSize	15
			fontName	"Dialog"
			autoSizePolicy	"node_width"
			anchor	"t"
			borderDistance	0.0
		]
		isGroup	1
	]
EOT

        $lgroup =~ s/\n+$/\n/;
        $output .= $lgroup;
    }

    # ID to node mapping
    my %ids;
    foreach my $schema ( sort keys %nodes ) {
        next if ( $schema eq $schema_name );
        foreach my $name ( sort keys %{ $nodes{$schema} } ) {
            $id++;
            $ids{$id} = "$schema.$name";
            $ids{"$schema.$name"} = $id;
        }
    }
    foreach my $name ( sort keys %{ $nodes{$schema_name} } ) {
        $id++;
        $ids{$id} = "$schema_name.$name";
        $ids{"$schema_name.$name"} = $id;
    }

    my $text;
    foreach my $schema ( sort keys %nodes ) {
        next if ( $schema eq $schema_name );
        $y0 = $groups{$schema}{y0};
        foreach my $name ( sort keys %{ $nodes{$schema} } ) {
            $id = $ids{"$schema.$name"};

            my $node = $nodes{$schema}{$name};
            ( $text, $y0 ) = $self->_node_gml(
                $name, $node, $id, $groups{$schema}{id},
                $y0,
                $groups{$schema}{x0},
                $groups{$schema}{x}
            );
            $y0 += $y_sep;
            $output .= $text;
        }
    }
    $y0 = 300;
    $x0 = 1000;
    foreach my $name ( sort keys %{ $nodes{$schema_name} } ) {
        $id = $ids{"$schema_name.$name"};
        my $node = $nodes{$schema_name}{$name};
        ( $text, $y0 ) = $self->_node_gml( $name, $node, $id, undef, $y0, $x0 );
        $y0 += $y_sep;
        $output .= $text;
    }

    foreach my $schema ( sort keys %nodes ) {
        foreach my $name ( sort keys %{ $nodes{$schema} } ) {
            my $id = $ids{"$schema.$name"};
            foreach my $link_schema ( sort keys %{ $nodes{$schema}{$name}{edges} } ) {
                foreach my $link_name ( sort keys %{ $nodes{$schema}{$name}{edges}{$link_schema} } ) {
                    my $end_id = $ids{"$link_schema.$link_name"};

                    my $edge_text .= << "EOT";
	edge
	[
		source	$id
		target	$end_id
		graphics
		[
			fill	"#000000"
			targetArrow	"standard"
		]
		edgeAnchor
		[
			ySource	-1.0
			yTarget	1.0
		]
	]
EOT

                    $output .= $edge_text;
                }
            }
        }
    }

    $output .= "]\n";

    return $output;
}

sub _graph_data {
    my ($self) = @_;

    my $schema_name = $self->{schema};
    my $objects     = $self->{objects};

    my %nodes;

    # Nodes for schema tables/views/materialized views
    foreach my $table_name ( sort keys %{ $objects->{'TABLE'} } ) {
        my $table_type = $objects->{'TABLE'}{$table_name}{table_type};
        $nodes{$schema_name}{$table_name}{node_comment} = $objects->{'TABLE'}{$table_name}{comments} || '';
        $nodes{$schema_name}{$table_name}{node_type}    = uc $table_type;
        $nodes{$schema_name}{$table_name}{row_count}    = $objects->{'TABLE'}{$table_name}{row_count} || '';
        $nodes{$schema_name}{$table_name}{width}        = $self->text_width( 'Dialog', 13, 'bold', $table_name ) + 16;

        my %pk_columns;
        if ( exists $objects->{'PRIMARY KEY'}{$table_name}{column_names} ) {
            %pk_columns = map { $_ => 1 } @{ $objects->{'PRIMARY KEY'}{$table_name}{column_names} };
        }

        my $label_width = 0;
        my $type_width  = 0;

        my @column_names = @{ $objects->{'COLUMN'}{$table_name}{column_names} };
        my @data_types   = @{ $objects->{'COLUMN'}{$table_name}{data_types} };

        $nodes{$schema_name}{$table_name}{height} = 29.1264648438 + ( 17.6357421875 * ( scalar @column_names ) );

        foreach my $idx ( 0 .. $#column_names ) {
            my $column_name = $column_names[$idx];
            my $data_type   = $data_types[$idx];
            my $position    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{ordinal_position};
            my $nullable    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{is_nullable};
            my $default     = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{data_default};
            my $comments    = $objects->{'COLUMN'}{$table_name}{columns}{$column_name}{comments};

            $nodes{$schema_name}{$table_name}{columns}{$position}{column_name} = $column_name;
            $nodes{$schema_name}{$table_name}{columns}{$position}{data_type}   = $data_type;
            $nodes{$schema_name}{$table_name}{columns}{$position}{primary_key} =
                ( exists $pk_columns{$column_name} ) ? '*' : '';
            $nodes{$schema_name}{$table_name}{columns}{$position}{nullable} = $nullable;
            $nodes{$schema_name}{$table_name}{columns}{$position}{y} =
                29.1264648438 + ( 17.6357421875 * ( $position - 1 ) );

            my $temp_width = $self->text_width( 'Helvetica', 10, 'normal', $column_name );
            $label_width = $temp_width if ( $temp_width > $label_width );

            $temp_width = $self->text_width( 'Helvetica', 10, 'normal', $data_type );
            $type_width = $temp_width if ( $temp_width > $type_width );
        }

        my $temp_width = 24 + 16 + 16 + $label_width + $type_width;

        $nodes{$schema_name}{$table_name}{width} = $temp_width
            if ( $temp_width > $nodes{$schema_name}{$table_name}{width} );
        $nodes{$schema_name}{$table_name}{datatype_offset} = $label_width + 24 + 16;
    }

    # Add edges
    if ( exists $objects->{'DEPENDENCY'} ) {
        foreach my $object_name ( sort keys %{ $objects->{'DEPENDENCY'} } ) {
            my @dependencies = @{ $objects->{'DEPENDENCY'}{$object_name} };
            foreach my $dependency (@dependencies) {

                unless ( exists $nodes{$schema_name}{$object_name} ) {
                    my $object_type = $dependency->{type};
                    $nodes{$schema_name}{$object_name}{height}    = 29.1264648438;
                    $nodes{$schema_name}{$object_name}{node_type} = uc $object_type;
                    $nodes{$schema_name}{$object_name}{width} =
                        $self->text_width( 'Dialog', 13, 'bold', $object_name ) + 16;
                }

                my $dep_schema = $dependency->{referenced_schema};
                my $dep_name   = $dependency->{referenced_name};
                my $dep_type   = $dependency->{referenced_type};

                unless ( exists $nodes{$dep_schema}{$dep_name} ) {
                    $nodes{$dep_schema}{$dep_name}{height}    = 29.1264648438;
                    $nodes{$dep_schema}{$dep_name}{node_type} = uc $dep_type;
                    $nodes{$dep_schema}{$dep_name}{width} = $self->text_width( 'Dialog', 13, 'bold', $dep_name ) + 16;
                }

                $nodes{$dep_schema}{$dep_name}{edges}{$schema_name}{$object_name} = 1;
            }
        }
    }

    if ( exists $objects->{'DEPENDENT'} ) {
        foreach my $object_name ( sort keys %{ $objects->{'DEPENDENT'} } ) {

            my @dependents = @{ $objects->{'DEPENDENT'}{$object_name} };
            foreach my $dependent (@dependents) {

                unless ( exists $nodes{$schema_name}{$object_name} ) {
                    my $object_type = $dependent->{type};
                    $nodes{$schema_name}{$object_name}{height}    = 29.1264648438;
                    $nodes{$schema_name}{$object_name}{node_type} = uc $object_type;
                    $nodes{$schema_name}{$object_name}{width} =
                        $self->text_width( 'Dialog', 13, 'bold', $object_name ) + 16;
                }

                my $dep_schema = $dependent->{referenced_schema};
                my $dep_name   = $dependent->{referenced_name};
                my $dep_type   = $dependent->{referenced_type};

                unless ( exists $nodes{$dep_schema}{$dep_name} ) {
                    $nodes{$dep_schema}{$dep_name}{height}    = 29.1264648438;
                    $nodes{$dep_schema}{$dep_name}{node_type} = uc $dep_type;
                    $nodes{$dep_schema}{$dep_name}{width} = $self->text_width( 'Dialog', 13, 'bold', $dep_name ) + 16;
                }

                $nodes{$schema_name}{$object_name}{edges}{$dep_schema}{$dep_name} = 1;
            }
        }
    }
    return %nodes;
}

sub _node_gml {
    my ( $self, $node_name, $node, $id, $gid, $y0, $x0, $x ) = @_;

    my $width     = $node->{width};
    my $height    = $node->{height};
    my $node_type = uc $node->{node_type} || 'default';
    my $color     = $self->node_color($node_type);
    my $shape     = $self->node_shape($node_type);

    my $y = $y0 + $height / 2;
    $x ||= $width / 2 + $x0;

    my $node_text = << "EOT";
	node
	[
		id	$id
		graphics
		[
			x	$x
			y	$y
			w	$width
			h	$height
			type	"$shape"
			fill	"$color"
			outline	"#000000"
		]
		LabelGraphics
		[
			text	"$node_name"
			fontSize	13
			fontStyle	"bold"
			fontName	"Dialog"
			anchor	"t"
		]
EOT

    if ($gid) {
        $node_text .= "		gid	" . $gid . "\n";
    }

    my $xk = $x0 + 8;
    my $xl = $x0 + 16;
    my $do = $node->{datatype_offset} || 0;
    my $xd = $x0 + $do;

    foreach my $col_id ( sort { $a <=> $b } keys %{ $node->{columns} } ) {

        my $yn          = $y + $node->{columns}{$col_id}{y} - $height / 2;
        my $font_style  = ( $node->{columns}{$col_id}{nullable} eq 'N' ) ? 'bold' : 'normal';
        my $column_name = $node->{columns}{$col_id}{column_name};
        my $data_type   = $node->{columns}{$col_id}{data_type};

        if ( $node->{columns}{$col_id}{primary_key} ) {
            $node_text .= << "EOT";
		LabelGraphics
		[
			text	"*"
			fontSize	10
			fontStyle	"bold"
			fontName	"Helvetica"
			x	$xk
			y	$yn
		]
EOT
        }

        $node_text .= << "EOT";
		LabelGraphics
		[
			text	"$column_name"
			fontSize	10
			fontStyle	"$font_style"
			fontName	"Helvetica"
			x	$xl
			y	$yn
		]
		LabelGraphics
		[
			text	"$data_type"
			fontSize	10
			fontStyle	"$font_style"
			fontName	"Helvetica"
			x	$xd
			y	$yn
		]
EOT

    }

    $node_text .= << "EOT";
	]
EOT

    $node_text =~ s/\n+$/\n/;

    return ( $node_text, $y0 + $height );
}

sub _legend {
    my ( $self, $id, $y_sep ) = @_;

    my $legend;
    my $x0    = 0;
    my $y0    = 300;
    my $width = 250;
    my $x     = $x0 + $width / 2;
    my $y     = 0;

    my @node_types = $self->node_types();

    foreach my $type (@node_types) {
        next if ( $type eq 'default' );
        my $color = $self->node_color($type);
        my $shape = $self->node_shape($type);
        $id++;
        $y = $y0 + $id * 50;
        $legend .= << "EOT";
	node
	[
		id	$id
		label	"$type"
		graphics
		[
			x	$x
			y	$y
			w	191.94140817732347
			h	30.0
			type	"$shape"
			fill	"$color"
			outline	"#000000"
		]
		LabelGraphics
		[
			text	"$type"
			fontSize	13
			fontName	"Dialog"
			anchor	"c"
			borderDistance	0.0
		]
		gid	0
	]
EOT
    }

    my $yg     = ( $y + $y0 ) / 2;
    my $h      = $id * 50 + $y_sep;
    my $lgroup = << "EOT";
Creator	"dep_graph"
graph
[
	label	""
	directed	1
	node
	[
		id	0
		label	"Legend"
		graphics
		[
			x	$x
			y	$yg
			w	$width
			h	$h
			type	"roundrectangle"
			fill	"#CCFFCC"
			outline	"#666699"
			outlineStyle	"dotted"
			topBorderInset	0.0
			bottomBorderInset	13.853386622378821
			leftBorderInset	2.529295911338295
			rightBorderInset	0.0
		]
		LabelGraphics
		[
			text	"Legend"
			fill	"#99CCFF"
			fontSize	15
			fontName	"Dialog"
			autoSizePolicy	"node_width"
			anchor	"t"
			borderDistance	0.0
		]
		isGroup	1
	]
$legend
EOT

    $lgroup =~ s/\n+$/\n/g;
    return ( $lgroup, $id );
}

sub _title_block {
    my ( $self, $id ) = @_;

    my $x0 = 0;
    my $y0 = 0;

    my $width = 900.0;

    my $xl      = $x0 + 20.0;
    my $xv      = $x0 + 200.0;
    my $yl      = $y0 + 75.0;
    my $delta_l = 25.0;
    my $height  = $yl + 6 * $delta_l;

    my $x = $x0 + $width / 2;
    my $y = $y0 + $height / 2;

    my $title_block = << "EOT";
	node
	[
		id	$id
		label	"Database Dependency Graph"
		graphics
		[
			x	$x
			y	$y
			w	$width
			h	$height
			type	"rectangle"
			fill	"#F0F0F0"
			outline	"#000000"
		]
		LabelGraphics
		[
			text	"Database Dependency Graph"
			fontSize	36
			fontStyle	"bold"
			fontName	"Dialog"
			anchor	"t"
		]
EOT

    my %tb_items = (
        'Created'          => $self->{date_time}      || '',
        'Database'         => $self->{database_name}  || '',
        'Database Version' => $self->{db_version}     || '',
        'Database Comment' => $self->{db_comment}     || '',
        'Schema'           => $self->{schema}         || '',
        'Schema Comment'   => $self->{schema_comment} || '',
    );

    foreach my $label ( sort keys %tb_items ) {
        my $value = $tb_items{$label};

        $title_block .= << "EOT";
		LabelGraphics
		[
			text	"$label:"
			fontSize	14
			fontStyle	"bold"
			fontName	"Helvetica"
			x	$xl
			y	$yl
		]
		LabelGraphics
		[
			text	"$value"
			fontSize	14
			fontStyle	"normal"
			fontName	"Helvetica"
			x	$xv
			y	$yl
		]
EOT
        $yl += $delta_l;
    }

    $title_block .= "	]\n";
    $title_block =~ s/\n+$/\n/g;
    return ( $title_block, $id );
}

1;
