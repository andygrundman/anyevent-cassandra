package AnyEvent::Cassandra;

use strict;

use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Cassandra::API::Cassandra;
use AnyEvent::Cassandra::Thrift::MemoryBuffer;
use AnyEvent::Cassandra::Thrift::BinaryProtocol;

use Bit::Vector;
use Scalar::Util qw(blessed);
use Time::HiRes;

sub new {
    my ( $class, %opts ) = @_;
    
    die 'host is required' unless $opts{host};
    
    if ( !ref $opts{host} ) {
        $opts{host} = [ $opts{host} ];
    }
    
    $opts{hosts} = [];
    for ( @{ delete $opts{host} } ) {
        my ($addr, $port) = split /:/;
        push @{ $opts{hosts} }, [ split /:/ ];
    }
    
    my $self = bless {
        hosts          => $opts{hosts},
        keyspace       => $opts{keyspace} || '',
        ts_func        => $opts{timestamp_func} || \&_ts_func,
        connected      => 0,
        cf_metadata    => {},
        auto_reconnect => exists $opts{auto_reconnect} ? $opts{auto_reconnect} : 1,
        max_retries    => $opts{max_retries} || 1,
        buffer_size    => $opts{buffer_size} || 8192,
        timeout        => $opts{timeout}     || 30,
        debug          => $opts{debug}       || 0,
    }, $class;
    
    $self->{transport} = AnyEvent::Cassandra::Thrift::MemoryBuffer->new( $self->{buffer_size} );
    $self->{protocol}  = AnyEvent::Cassandra::Thrift::BinaryProtocol->new( $self->{transport} );
    $self->{api}       = AnyEvent::Cassandra::API::CassandraClient->new( $self->{protocol} );
    
    return $self;
}

sub connect {
    my ( $self, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    my $t = AnyEvent->timer( after => $self->{timeout}, cb => sub {
        $self->{debug} && warn "<< connect [TIMEOUT]\n";
        $cb->(0, 'Connect timed out');
        $self->{handle} && $self->{handle}->destroy;
    } );
    
    my $ts;
    $self->{debug} && ($ts = Time::HiRes::time()) && warn ">> connect\n";
    
    $self->{handle} = AnyEvent::Handle->new(
        connect    => @{ $self->{hosts} }[ rand $#{$self->{hosts}} ],
        keepalive  => 1,
        no_delay   => 1,
        on_connect => sub {
            $self->{debug} && warn "<< connected (" . sprintf("%.1f", (Time::HiRes::time() - $ts) * 1000) . " ms)\n";
            
            undef $t;
            $self->{connected} = 1;
            
            if ( $self->{keyspace} ) {
                $self->set_keyspace( [ $self->{keyspace} ], $cb );
                return;
            }
            
            $cb->(1);
        },
        on_error   => sub {
            $self->{debug} && warn "<< connect [ERROR] $_[2]\n";
            
            undef $t;
            $self->{connected} = 0;
            
            $cb->(0, $_[2]);
            $_[0]->destroy;
            delete $self->{handle};
        },
        on_read    => sub { },
    );
    
    return $cb;
}

sub close {
    my $self = shift;
    
    if ( defined $self->{handle} ) {
        $self->{debug} && warn ">> close\n";
        $self->{handle}->push_shutdown;
        $self->{connected} = 0;
    }
}

sub _call {
    my ( $self, $method, $args, $cb, $retry ) = @_;
    
    # Strip leading underscore needed for some methods
    $method =~ s/^_//;
    
    if ( !$self->{connected} ) {
        if ( $self->{auto_reconnect} ) {
            $retry ||= 0;
            
            if ( $retry > $self->{max_retries} ) {
                $self->{debug} && warn "<< max_retries reached, unable to auto-reconnect\n";
                $cb->(0, "max_retries reached, unable to auto-reconnect");
            }
            
            $self->{debug} && warn ">> $method [NOT CONNECTED] will auto-reconnect\n";
            $self->connect( sub {
                my ($ok, $error) = @_;
                if ( !$ok ) {
                    $self->{debug} && warn "<< auto-reconnect [ERROR] $error\n";
                    $cb->(0, "auto-reconnect failed ($error)");
                }
                else {
                    # Retry the call
                    ++$retry;
                    $self->_call( $method, $args, $cb, $retry );
                }
            } );
        }
        else {
            $self->{debug} && warn ">> $method [ERROR] not connected\n";
            $cb->(0, "not connected, you may want to enable auto_reconnect => 1");
        }
        
        return;
    }
    
    my $ts;
    $self->{debug} && ($ts = Time::HiRes::time()) && warn ">> $method " . ($retry ? "[RETRY $retry]" : "") . "\n";
    
    my $handle = $self->{handle};
    my $membuf = $self->{transport};
    
    my $send = "send_${method}";
    my $recv = "recv_${method}";
    
    $args ||= [];
    $cb   ||= AnyEvent->condvar;
    
    my $t = AnyEvent->timer( after => $self->{timeout}, cb => sub {
        $self->{debug} && warn "<< $method [TIMEOUT]\n";
        $cb->(0, "Request $method timed out");
    } );
    
    $self->{api}->$send( @{$args} );
    
    # Write in framed format
    my $len = $membuf->available();
    $handle->push_write( pack('N', $len) . $membuf->read($len) );
    
    # Read frame length
    $handle->push_read( chunk => 4, sub {
        my $len = unpack 'N', $_[1];
        
        # Read frame data
        $handle->unshift_read( chunk => $len, sub {
            undef $t;
            
            $membuf->write($_[1]);
            
            my $result = eval { $self->{api}->$recv() };

            if ( $@ ) {
                $self->{debug} && do {
                    require Data::Dump;
                    warn "<< $method [ERROR] " . Data::Dump::dump($@) . "\n";
                };
                $cb->(0, $@);
            }
            else {
                $self->{debug} && warn "<< $method OK (" . sprintf("%.1f", (Time::HiRes::time() - $ts) * 1000) . " ms)\n";
                $cb->(1, $result);
            }
        } );
    } );
    
    return $cb;
}

### API methods (simpler format)

=head2 insert_simple( $column_family => $key => \%data, [ $callback ] );

This is a simpler way of using L<batch_mutate> to insert many columns at once.

=cut

sub insert_simple {
    my ( $self, $cf, $key, $data, $cb ) = @_;
    
    my $ts = $self->{ts_func}->();
    
    my $metadata  = $self->{cf_metadata}->{$cf} || {};
    my $col_types = $metadata->{column_types} || {};
    
    my $mutation_list = [];
    if ( ref $data eq 'HASH' ) {
        while ( my ($k, $v) = each %{$data} ) {        
            push @{$mutation_list}, bless {
                column_or_supercolumn => bless {
                    column => bless {
                        name      => $k,
                        value     => $v,
                        timestamp => $ts,
                        _type     => $col_types->{$k}, # used for conversion
                    }, 'AnyEvent::Cassandra::API::Column',
                }, 'AnyEvent::Cassandra::API::ColumnOrSuperColumn',
            }, 'AnyEvent::Cassandra::API::Mutation';
        }
    }
    elsif ( ref $data eq 'ARRAY' ) {
        # Valueless key
        for my $k ( @{$data} ) {
            push @{$mutation_list}, bless {
                column_or_supercolumn => bless {
                    column => bless {
                        name      => $k,
                        value     => '',
                        timestamp => $ts,
                    }, 'AnyEvent::Cassandra::API::Column',
                }, 'AnyEvent::Cassandra::API::ColumnOrSuperColumn',
            }, 'AnyEvent::Cassandra::API::Mutation';
        }
    }
    
    # XXX convert $key type?
    
    my $mutation_map = {
        $key => {
            $cf => $mutation_list,
        },
    };
    
    return $self->batch_mutate( [ $mutation_map ], $cb );
}

# Hack the API::Column auto-generated write method to handle data type conversion
# Unfortunately we can't also do this for read() because it doesn't have the type
# or even column family information.
*AnyEvent::Cassandra::API::Column::write = sub {
    my ( $self, $output ) = @_;
    my $xfer = 0;
    $xfer += $output->writeStructBegin('Column');
    if ( defined $self->{name} ) {
        $xfer += $output->writeFieldBegin( 'name', TType::STRING, 1 );
        $xfer += $output->writeString( $self->{name} );
        $xfer += $output->writeFieldEnd();
    }
    if ( defined $self->{value} ) {
        $xfer += $output->writeFieldBegin( 'value', TType::STRING, 2 );
        if ( my $type = $self->{_type} ) {
            $xfer += $output->writeString( _convert_to( $type, $self->{value} ) );
        }
        else {
            $xfer += $output->writeString( $self->{value} );
        }

        $xfer += $output->writeFieldEnd();
    }
    if ( defined $self->{timestamp} ) {
        $xfer += $output->writeFieldBegin( 'timestamp', TType::I64, 3 );
        $xfer += $output->writeI64( $self->{timestamp} );
        $xfer += $output->writeFieldEnd();
    }
    if ( defined $self->{ttl} ) {
        $xfer += $output->writeFieldBegin( 'ttl', TType::I32, 4 );
        $xfer += $output->writeI32( $self->{ttl} );
        $xfer += $output->writeFieldEnd();
    }
    $xfer += $output->writeFieldStop();
    $xfer += $output->writeStructEnd();
    return $xfer;
};

### API methods (native format)
# Methods with underscores are called by wrappers that handle datatype conversion

my @methods = qw(
    login
    _get
    _get_slice
    get_count
    _multiget_slice
    multiget_count
    _get_range_slices
    _get_indexed_slices
    insert
    remove
    batch_mutate
    truncate
    
    describe_schema_versions
    describe_keyspaces
    describe_version
    describe_cluster_name
    describe_ring
    describe_partitioner
    describe_snitch
    describe_keyspace
    describe_splits
    
    system_add_column_family
    system_drop_column_family
    system_add_keyspace
    system_drop_keyspace
    system_update_keyspace
    system_update_column_family
);

# Mapping from API method to arg class names, needed so callers don't have
# to bother creating lots of objects
my %arg_class_map = (
    login               => [ 'AuthenticationRequest' ],
    _get                => [ undef, 'ColumnPath', 'ConsistencyLevel' ],
    _get_slice          => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ],
    get_count           => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ],
    _multiget_slice     => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ],
    multiget_count      => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ],
    _get_range_slices   => [ 'ColumnParent', 'SlicePredicate', 'KeyRange', 'ConsistencyLevel' ],
    _get_indexed_slices => [ 'ColumnParent', 'IndexClause', 'SlicePredicate', 'ConsistencyLevel' ],
    insert              => [ undef, 'ColumnParent', 'Column', 'ConsistencyLevel' ],
    remove              => [ undef, 'ColumnPath', undef, 'ConsistencyLevel' ],
    batch_mutate        => [ undef, 'ConsistencyLevel' ],
    
    system_add_column_family    => [ 'CfDef' ],
    system_add_keyspace         => [ 'KsDef' ],
    system_update_column_family => [ 'CfDef' ],
    system_update_keyspace      => [ 'KsDef' ],
);

my %nested_class_map = (
    KsDef => {
        cf_defs => 'CfDef',
    },
    CfDef => {
        column_metadata => 'ColumnDef',
    },
    SlicePredicate => {
        slice_range => 'SliceRange',
    },
    IndexClause => {
        expressions => 'IndexExpression',
    },
);

{
    no strict 'refs';
    for my $method ( @methods ) {
        # More complex wrapper for methods with object args
        if ( my $map = $arg_class_map{$method} ) {
            *{$method} = sub {
                my $args = ref $_[1] eq 'ARRAY' ? $_[1] : [ $_[1] ];
                my $i = 0;
                for my $arg ( @{$args} ) {
                    if ( defined $map->[$i] ) {
                        if ( ref $arg eq 'HASH' ) {
                            # Map any nested classes within this one
                            if ( my $smap = $nested_class_map{ $map->[$i] } ) {
                                while ( my ($k, $v) = each %{$smap} ) {                          
                                    if ( my $item  = $arg->{$k} ) {
                                        my $class = 'AnyEvent::Cassandra::API::' . $v;
                                        
                                        if ( ref $item eq 'ARRAY' ) {
                                            # Map an array of hashrefs to an array of objects
                                            foreach ( @{$item} ) {
                                                $_ = bless $_, $class;
                                            }
                                        }
                                        else {
                                            # Map just the single object 
                                            $item = bless $item, $class;
                                        }
                                    }
                                }
                            }
                        }
                        
                        if ( !blessed($arg) ) {
                            $arg = bless $arg, 'AnyEvent::Cassandra::API::' . $map->[$i];
                        }
                    }
                    $i++;
                }
                
                $_[0]->_call( $method, $args, $_[2] );
            };
        }
        else {
            # Simple wrapper for methods without object args
            *{$method} = sub { $_[0]->_call( $method, ref $_[1] eq 'ARRAY' ? $_[1] : [ $_[1] ], $_[2] ); };
        }
    }
}

# get method wrapper to handle datatype conversion
sub get {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    $self->_get( $args, sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        if ( my $col = $res->{column} ) {
            my $metadata  = $self->{cf_metadata}->{ $args->[1]->{column_family} } || {};
            my $col_types = $metadata->{column_types} || {};
            
            if ( my $type = $col_types->{ $col->{name} } ) {
                $col->{value} = _convert_from( $type, $col->{value} );
            }
        }
        
        $cb->(@_);
    } );
    
    return $cb;
}

sub get_slice {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    $self->_get_slice( $args, sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        my $metadata  = $self->{cf_metadata}->{ $args->[1]->{column_family} } || {};
        my $col_types = $metadata->{column_types} || {};
        
        for my $cosc ( @{$res} ) {
            if ( my $col = $cosc->{column} ) {
                if ( my $type = $col_types->{ $col->{name} } ) {
                    $col->{value} = _convert_from( $type, $col->{value} );
                }
            }
        }
        
        $cb->(@_);
    } );
    
    return $cb;
}

sub multiget_slice {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    $self->_multiget_slice( $args, sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        my $metadata  = $self->{cf_metadata}->{ $args->[1]->{column_family} } || {};
        my $col_types = $metadata->{column_types} || {};
        
        while ( my ($key, $list) = each %{$res} ) {
            for my $cosc ( @{$list} ) {
                if ( my $col = $cosc->{column} ) {
                    if ( my $type = $col_types->{ $col->{name} } ) {
                        $col->{value} = _convert_from( $type, $col->{value} );
                    }
                }
            }
        }
        
        $cb->(@_);
    } );
    
    return $cb;
}

sub get_range_slices {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    $self->_get_range_slices( $args, sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        my $metadata  = $self->{cf_metadata}->{ $args->[0]->{column_family} } || {};
        my $col_types = $metadata->{column_types} || {};
        
        for my $slice ( @{$res} ) {
            for my $cosc ( @{ $slice->{columns} } ) {
                if ( my $col = $cosc->{column} ) {
                    if ( my $type = $col_types->{ $col->{name} } ) {
                        $col->{value} = _convert_from( $type, $col->{value} );
                    }
                }
            }
        }
        
        $cb->(@_);
    } );
    
    return $cb;
}

sub get_indexed_slices {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    $self->_get_indexed_slices( $args, sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        my $metadata  = $self->{cf_metadata}->{ $args->[0]->{column_family} } || {};
        my $col_types = $metadata->{column_types} || {};
        
        for my $slice ( @{$res} ) {
            for my $cosc ( @{ $slice->{columns} } ) {
                if ( my $col = $cosc->{column} ) {
                    if ( my $type = $col_types->{ $col->{name} } ) {
                        $col->{value} = _convert_from( $type, $col->{value} );
                    }
                }
            }
        }
        
        $cb->(@_);
    } );
    
    return $cb;
}

# set_keyspace needs to run describe_keyspace to get metadata about the keyspace
sub set_keyspace {
    my ( $self, $args, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    my $keyspace = ref $args eq 'ARRAY' ? $args->[0] : $args;
    
    $self->{keyspace} = $keyspace;
    
    $self->_call( 'set_keyspace', [ $keyspace ], sub {
        my ($ok, $res) = @_;
        return $cb->(@_) if !$ok;
        
        $self->describe_keyspace( [ $keyspace ], sub {
            my ($ok, $res) = @_;
            return $cb->(@_) if !$ok;
            
            # Store info about all columns marked with a validation_class
            # so that we can properly convert data for those columns
            my $cf_metadata = {};
            for my $cf ( @{ $res->{cf_defs} } ) {
                my $columns = {};
                
                for my $col ( @{ $cf->{column_metadata} || [] } ) {
                    if ( my ($vc) = $col->{validation_class} =~ /org.apache.cassandra.db.marshal.(\w+)/ ) {
                        # Ignore types we don't have to convert
                        next if $vc =~ /^(?:Ascii|Bytes|UTF8)/;
                        
                        $columns->{ $col->{name} } = $vc;
                    }
                }
                
                # comparator_type is the data type for all keys
                my ($comparator_type) = $cf->{comparator_type} =~ /org.apache.cassandra.db.marshal.(\w+)/;
                
                # Ignore types we don't have to convert
                if ( !$comparator_type || $comparator_type =~ /^(?:Ascii|Bytes|UTF8)/ ) {
                    $comparator_type = 0;
                }
                
                $cf_metadata->{ $cf->{name} } = {
                    key_type     => $comparator_type,
                    column_types => $columns,
                };
            }
            
            $self->{cf_metadata} = $cf_metadata;
            
            $cb->(@_);
        } );
    } );
    
    return $cb;
}

# Default timestamp function, creates a 64-bit int from HiRes time by simply removing the decimal point
sub _ts_func {
    my $ts = sprintf "%.06f", Time::HiRes::time();
    $ts =~ s/\.//;
    
    return $ts;
}

# Convert to a given Cassandra type
sub _convert_to {
    my ( $type, $value ) = @_;
    
    if ( $type eq 'IntegerType' ) {
        if ( abs($value) < 1 << 31 ) { # number fits into signed 32-bit
            $value = pack 'N!', $value;
        }
        else {
            my $vec = Bit::Vector->new_Dec(64, $value);
            $value = pack 'NN', $vec->Chunk_Read(32, 32), $vec->Chunk_Read(32, 0);
        }
    }
    elsif ( $type eq 'LongType' ) {
        my $vec = Bit::Vector->new_Dec(64, $value);
        $value = pack 'NN', $vec->Chunk_Read(32, 32), $vec->Chunk_Read(32, 0);
    }
    elsif ( $type eq 'LexicalUUIDType' || $type eq 'TimeUUIDType' ) {
        $value = pack 'H*', $value;
    }
    
    return $value;
}

# Convert from a given Cassandra type
sub _convert_from {
    my ( $type, $value ) = @_;
    
    if ( $type eq 'IntegerType' ) {
        if ( length($value) == 4 ) {
            $value = unpack 'N!', $value;
        }
        elsif ( length($value) == 8 ) {
            my $vec = Bit::Vector->new_Hex(64, unpack('H*', $value));
            $value = $vec->to_Dec();
        }
    }
    elsif ( $type eq 'LongType' ) {
        my $vec = Bit::Vector->new_Hex(64, unpack('H*', $value));
        $value = $vec->to_Dec();
    }
    elsif ( $type eq 'LexicalUUIDType' || $type eq 'TimeUUIDType' ) {
        $value = unpack 'H*', $value;
    }
    
    return $value;
}

1;
