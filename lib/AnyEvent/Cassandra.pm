package AnyEvent::Cassandra;

use strict;

use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Cassandra::API::Cassandra;
use AnyEvent::Cassandra::Thrift::MemoryBuffer;
use AnyEvent::Cassandra::Thrift::BinaryProtocol;

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
                $self->set_keyspace( [ $self->{keyspace} ], sub {
                    $cb->(shift); # return set_keyspace ok/fail to caller
                } );
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
    
    # Assume "send_foo" and "recv_foo" convention
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
    
    my $mutation_list = [];
    while ( my ($k, $v) = each %{$data} ) {
        push @{$mutation_list}, bless {
            column_or_supercolumn => bless {
                column => bless {
                    name      => $k,
                    value     => $v,
                    timestamp => $ts,
                }, 'AnyEvent::Cassandra::API::Column',
            }, 'AnyEvent::Cassandra::API::ColumnOrSuperColumn',
        }, 'AnyEvent::Cassandra::API::Mutation';
    }
    
    my $mutation_map = {
        $key => {
            $cf => $mutation_list,
        },
    };
    
    return $self->batch_mutate( [ $mutation_map ], $cb );
}

### API methods (native format)

my @methods = qw(
    login
    set_keyspace
    get
    get_slice
    get_count
    multiget_slice
    multiget_count
    get_range_slices
    get_indexed_slices
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
    login              => [ 'AuthenticationRequest' ],
    get                => [ undef, 'ColumnPath', 'ConsistencyLevel' ],
    get_slice          => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ], # XXX SlicePredicate has nested SliceRange
    get_count          => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ], # XXX ''
    multiget_slice     => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ], # XXX ''
    multiget_count     => [ undef, 'ColumnParent', 'SlicePredicate', 'ConsistencyLevel' ], # XXX ''
    get_range_slices   => [ 'ColumnParent', 'SlicePredicate', 'KeyRange', 'ConsistencyLevel' ], # XXX ''
    get_indexed_slices => [ 'ColumnParent', 'IndexClause', 'SlicePredicate', 'ConsistencyLevel' ], # XXX '', IndexClause has nested IndexExpression which has nested IndexOperator
    insert             => [ undef, 'ColumnParent', 'Column', 'ConsistencyLevel' ],
    remove             => [ undef, 'ColumnPath', undef, 'ConsistencyLevel' ],
    batch_mutate       => [ undef, 'ConsistencyLevel' ],
    
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

# Default timestamp function, creates a 64-bit int from HiRes time by simply removing the decimal point
sub _ts_func {
    my $ts = sprintf "%.06f", Time::HiRes::time();
    $ts =~ s/\.//;
    
    return $ts;
}

1;
