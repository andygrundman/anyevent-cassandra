package AnyEvent::Cassandra;

use strict;

use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Cassandra::API::Cassandra;
use AnyEvent::Cassandra::Thrift::MemoryBuffer;
use AnyEvent::Cassandra::Thrift::BinaryProtocol;

use Scalar::Util qw(blessed);

sub new {
    my ( $class, %opts ) = @_;
    
    $opts{timeout}     ||= 30;
    $opts{buffer_size} ||= 8192;
    
    die 'host is required' unless $opts{host};
    
    if ( !ref $opts{host} ) {
        $opts{host} = [ $opts{host} ];
    }
    
    $opts{hosts} = [];
    for ( @{ delete $opts{host} } ) {
        my ($addr, $port) = split /:/;
        push @{ $opts{hosts} }, [ split /:/ ];
    }
    
    my $self = bless \%opts, $class;
    
    $self->{transport} = AnyEvent::Cassandra::Thrift::MemoryBuffer->new( $self->{buffer_size} );
    $self->{protocol}  = AnyEvent::Cassandra::Thrift::BinaryProtocol->new( $self->{transport} );
    $self->{api}       = AnyEvent::Cassandra::API::CassandraClient->new( $self->{protocol} );
    
    return $self;
}

sub connect {
    my ( $self, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    my $t = AnyEvent->timer( after => $self->{timeout}, cb => sub {
        $cb->(0, 'Connect timed out');
    } );
    
    $self->{handle} = AnyEvent::Handle->new(
        connect    => @{ $self->{hosts} }[rand $#{$self->{hosts}} ],
        on_connect => sub { undef $t; $cb->(1) },
        on_error   => sub { undef $t; $cb->(0, $_[2]); },
        on_read    => sub { },
        no_delay   => 1, # XXX needed?
    );
    
    return $cb;
}

sub close {
    my $self = shift;
    
    if ( defined $self->{handle} ) {
        $self->{handle}->push_shutdown;
    }
}

sub _call {
    my ( $self, $method, $args, $cb ) = @_;
    
    my $handle = $self->{handle};
    my $membuf = $self->{transport};
    
    # Assume "send_foo" and "recv_foo" convention
    my $send = "send_${method}";
    my $recv = "recv_${method}";
    
    $args ||= [];
    $cb   ||= AnyEvent->condvar;
    
    my $t = AnyEvent->timer( after => $self->{timeout}, cb => sub {
        $cb->(0, "Request $send timed out");
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
                $cb->(0, $@);
            }
            else {
                $cb->(1, $result);
            }
        } );
    } );
    
    return $cb;
}        

### API methods

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
    batch_mutate       => [], # XXX map<binary, map<string, list<Mutation>>>
    
    system_add_column_family    => [ 'CfDef' ],
    system_add_keyspace         => [ 'KsDef' ],
    system_update_column_family => [ 'CfDef' ],
    system_update_keyspace      => [ 'KsDef' ],
);

{
    no strict 'refs';
    for my $method ( @methods ) {
        # More complex wrapper for methods with object args
        if ( my $map = $arg_class_map{$method} ) {
            *{$method} = sub {
                my $args = [];
                my $i = 0;
                for my $arg ( @{ $_[1] } ) {
                    if ( defined $map->[$i] && !blessed($arg) ) {
                        $arg = bless $arg, 'AnyEvent::Cassandra::API::' . $map->[$i];
                    }
                    $i++;
                }
                
                $_[0]->_call( $method, $_[1], $_[2] );
            };
        }
        else {
            # Simple wrapper for methods without object args
            *{$method} = sub { $_[0]->_call( $method, $_[1], $_[2] ); };
        }
    }
}

1;
