# Tests for simple describe_* API calls, all made in sync mode

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
    ? ( tests => 15 )
    : ( skip_all => "No local Cassandra server found on ${host}:${port}, please start one or set CASSANDRA_HOST and CASSANDRA_PORT" );

ok( setup_keyspace($host, $port), "Test keyspace created" );

my $client = get_client();

# describe_schema_versions
{
    my ($ok, $res) = $client->describe_schema_versions();
    is( $ok, 1, 'describe_schema_versions ok' );
    like( (keys %{$res})[0], qr/^[0-9a-f\-]+$/, 'describe_schema_versions result ok' );
}

# describe_keyspaces
{
    my ($ok, $res) = $client->describe_keyspaces();
    is( $ok, 1, 'describe_keyspaces ok' );
    
    my ($ksdef) = grep { $_->{name} eq 'AECTestKeyspace' } @{$res};
    is( $ksdef->replication_factor, 1, 'describe_keyspaces replication_factor ok' );
}

# describe_version
{
    my ($ok, $res) = $client->describe_version();
    is( $ok, 1, 'describe_version ok' );
    like( $res, qr/^[0-9\.]+$/, "describe_version $res ok" );
}

# describe_cluster_name
{
    my ($ok, $res) = $client->describe_cluster_name();
    is( $ok, 1, "describe_cluster_name $res ok" );
}

# describe_ring
{
    my ($ok, $res) = $client->describe_ring('AECTestKeyspace');
    is( $ok, 1, 'describe_ring ok' );
    like( $res->[0]->start_token, qr/^[0-9]+$/, 'describe_ring start_token ok' );
}

# describe_partitioner
{
    my ($ok, $res) = $client->describe_partitioner();
    is( $ok, 1, "describe_partitioner $res ok" );
}

# describe_snitch
{
    my ($ok, $res) = $client->describe_snitch();
    is( $ok, 1, "describe_snitch $res ok" );
}

# describe_keyspace
{
    my ($ok, $res) = $client->describe_keyspace('AECTestKeyspace');
    is( $ok, 1, 'describe_keyspace ok' );
    is( $res->replication_factor, 1, 'describe_keyspace replication_factor ok' );
}

# describe_splits not tested, it's marked experimental

ok( cleanup_keyspace($host, $port), "Test keyspace cleaned up" );
