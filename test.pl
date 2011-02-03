#!/usr/bin/perl

use strict;
use lib qw(lib);

use EV;
use AnyEvent;
use AnyEvent::Cassandra;
use Data::Dump qw(dump);
use Tie::IxHash;

my $async = shift; # test async or sync operation
if (!defined $async) { $async = 1 }

my $client = AnyEvent::Cassandra->new(
	host       => '192.168.100.10:9160',
	keyspace   => 'MyKeyspace',
	timeout    => 2,
);

if ($async) {
    # Async
    my $cv = AnyEvent->condvar;
    
    $client->connect( sub {
        my ($status, $error) = @_;
        warn "Connected async: $status ($error)\n";
        if (!$status) {
            warn "Lost connection\n";
            exit;
        }
        $cv->send;
    } );
    
    $cv->recv;
}
else {
    # Sync
    my ($status, $error) = $client->connect->recv;
    warn "Connected sync: $status ($error)\n";
}

my $ts = time();

use constant EQ => 0;

my $cf = 'AndyTest';

tie my %methods, 'Tie::IxHash', (
    describe_schema_versions => [],
    describe_keyspaces => [],
    describe_cluster_name => [],
    describe_version => [],
    describe_ring => [ 'MyKeyspace' ],
    describe_partitioner => [],
    describe_snitch => [],
    describe_keyspace => [ 'MyKeyspace' ],
    #describe_splits => [ $cf, "1", "1000", 100 ],
    
    login => [ { credentials => { 'foo', 'bar' } } ],
    set_keyspace => [ 'MyKeyspace' ],
    
    system_add_column_family => [ { keyspace => 'MyKeyspace', name => $cf } ],
    
    insert => [ 'key', { column_family => $cf }, { name => 'colname', value => 'colvalue', timestamp => $ts } ],
    get => [ 'key', { column_family => $cf, column => 'colname' } ],
    get_slice => [ 'key', { column_family => $cf }, { column_names => [ 'colname' ] } ],
    get_count => [ 'key', { column_family => $cf }, { column_names => [ 'colname' ] } ],
    multiget_slice => [ [ 'key' ], { column_family => $cf }, { column_names => [ 'colname' ] } ],
    multiget_count => [ [ 'key' ], { column_family => $cf }, { column_names => [ 'colname' ] } ],
    get_range_slices => [ { column_family => $cf }, { column_names => [ 'colname' ] }, { start_key => 'key', end_key => 'key', count => 100 } ],
    #get_indexed_slices => [ { column_family => $cf }, { expressions => [ { column_name => 'colname', op => EQ, value => 'colvalue' } ], start_key => 'key' }, { column_names => [ 'colname' ] } ],
    remove => [ 'key', { column_family => $cf }, $ts ],
    #batch_mutate
    truncate => [ $cf ],
);

while ( my ($method, $args) = each %methods ) {
    if ($async) {
        my $cv = AnyEvent->condvar;
    
        warn "Async method call: $method ( " . dump($args) . " )\n";
        $client->$method( $args, sub {
            my ($status, $res) = @_;
            warn "Async $method result: " . dump($res) . "\n";
            $cv->send;
        } );
    
        $cv->recv;
    }
    else {
        warn "Sync method call: $method ( " . dump($args) . " )\n";
        my ($status, $res) = $client->$method($args)->recv;
        warn "Sync $method result: " . dump($res) . "\n";
    }
}

warn "Closing\n";
$client->close;
