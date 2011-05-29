# Tests for simple describe_* API calls, made in sync mode followed by async mode

use strict;

use AnyEvent;
use AnyEvent::Cassandra;
use Data::Dump qw(dump);
use FindBin ();
use Test::More;

use lib "$FindBin::Bin/lib";
use AECTestUtil qw(check_server get_client setup_keyspace cleanup_keyspace);

my ($ok, $host, $port) = check_server();
plan $ok
    ? ( tests => 28 )
    : ( skip_all => "No local Cassandra server found on ${host}:${port}, please start one or set CASSANDRA_HOST and CASSANDRA_PORT" );

ok( setup_keyspace($host, $port), "Test keyspace created" );

my $client = get_client();

# describe_schema_versions
{
    my ($ok, $res) = $client->describe_schema_versions();
    is( $ok, 1, '[sync ] describe_schema_versions ok' );
    like( (keys %{$res})[0], qr/^[0-9a-f\-]+$/, '[sync ] describe_schema_versions result ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_schema_versions( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] describe_schema_versions ok' );
        like( (keys %{$res})[0], qr/^[0-9a-f\-]+$/, '[async] describe_schema_versions result ok' );
        $cv->send;
    } );
    $cv->recv;
}

# describe_keyspaces
{
    my ($ok, $res) = $client->describe_keyspaces();
    is( $ok, 1, '[sync ] describe_keyspaces ok' );
    
    my ($ksdef) = grep { $_->{name} eq 'AECTestKeyspace' } @{$res};
    is( $ksdef->replication_factor, 1, '[sync ] describe_keyspaces replication_factor ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_keyspaces( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] describe_keyspaces ok' );
    
        my ($ksdef) = grep { $_->{name} eq 'AECTestKeyspace' } @{$res};
        is( $ksdef->replication_factor, 1, '[async] describe_keyspaces replication_factor ok' );
        $cv->send;
    } );
    $cv->recv;
}

# describe_version
{
    my ($ok, $res) = $client->describe_version();
    is( $ok, 1, '[sync ] describe_version ok' );
    like( $res, qr/^[0-9\.]+$/, "[sync ] describe_version $res ok" );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_version( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] describe_version ok' );
        like( $res, qr/^[0-9\.]+$/, "[async] describe_version $res ok" );
        $cv->send;
    } );
    $cv->recv;
}

# describe_cluster_name
{
    my ($ok, $res) = $client->describe_cluster_name();
    is( $ok, 1, "[sync ] describe_cluster_name $res ok" );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_cluster_name( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, "[async] describe_cluster_name $res ok" );
        $cv->send;
    } );
    $cv->recv;
}

# describe_ring
{
    my ($ok, $res) = $client->describe_ring('AECTestKeyspace');
    is( $ok, 1, '[sync ] describe_ring ok' );
    like( $res->[0]->start_token, qr/^[0-9]+$/, '[sync ] describe_ring start_token ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_ring( 'AECTestKeyspace', sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] describe_ring ok' );
        like( $res->[0]->start_token, qr/^[0-9]+$/, '[async] describe_ring start_token ok' );
        $cv->send;
    } );
    $cv->recv;
}

# describe_partitioner
{
    my ($ok, $res) = $client->describe_partitioner();
    is( $ok, 1, "[sync ] describe_partitioner $res ok" );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_partitioner( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, "[async] describe_partitioner $res ok" );
        $cv->send;
    } );
    $cv->recv;
}

# describe_snitch
{
    my ($ok, $res) = $client->describe_snitch();
    is( $ok, 1, "[sync ] describe_snitch $res ok" );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_snitch( sub {
        my ($ok, $res) = @_;
        is( $ok, 1, "[async] describe_snitch $res ok" );
        $cv->send;
    } );
    $cv->recv;
}

# describe_keyspace
{
    my ($ok, $res) = $client->describe_keyspace('AECTestKeyspace');
    is( $ok, 1, '[sync ] describe_keyspace ok' );
    is( $res->replication_factor, 1, '[sync ] describe_keyspace replication_factor ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->describe_keyspace( 'AECTestKeyspace', sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] describe_keyspace ok' );
        is( $res->replication_factor, 1, '[async] describe_keyspace replication_factor ok' );
        $cv->send;
    } );
    $cv->recv;
}

# describe_splits not tested, it's marked experimental

ok( cleanup_keyspace($host, $port), "Test keyspace cleaned up" );
