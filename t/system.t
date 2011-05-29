# Tests for system API calls

use strict;

use AnyEvent;
use AnyEvent::Cassandra;
use Data::Dump qw(dump);
use FindBin ();
use Test::More;

use lib "$FindBin::Bin/lib";
use AECTestUtil qw(check_server get_client setup_keyspace cleanup_keyspace setup_column_family cleanup_column_family);

my ($ok, $host, $port) = check_server();
plan $ok
    ? ( tests => 8 )
    : ( skip_all => "No local Cassandra server found on ${host}:${port}, please start one or set CASSANDRA_HOST and CASSANDRA_PORT" );

ok( setup_keyspace($host, $port), "Test keyspace created" );
ok( setup_column_family('AECTestCF'), "Test column family created" );

my $client = get_client();

# system_update_keyspace
{
    my ($ok, $res) = $client->system_update_keyspace( {
        name               => 'AECTestKeyspace',
        strategy_class     => 'org.apache.cassandra.locator.SimpleStrategy',
        replication_factor => 2,
        cf_defs            => [],
    } );
    
    is( $ok, 1, 'system_update_keyspace ok' );
    
    # Verify the replication_factor changed
    ($ok, $res) = $client->describe_keyspace('AECTestKeyspace');
    is( $res->replication_factor, 2, 'system_update_keyspace changed replication_factor to 2 ok' );
}

# system_update_column_family
{
    # Get current CF details
    my ($ok, $res) = $client->describe_keyspace('AECTestKeyspace');
    my ($cf) = grep { $_->{name} eq 'AECTestCF' } @{ $res->{cf_defs} };
    
    my ($ok, $res) = $client->system_update_column_family( {
        keyspace => 'AECTestKeyspace',
        name     => 'AECTestCF',
        id       => $cf->id,
        comment  => 'updated comment',
    } );
    
    is( $ok, 1, 'system_update_column_family ok' );
    
    # Verify the comment
    ($ok, $res) = $client->describe_keyspace('AECTestKeyspace');
    ($cf) = grep { $_->{name} eq 'AECTestCF' } @{ $res->{cf_defs} };
    is( $cf->comment, 'updated comment', 'system_update_column_family comment changed ok' );
}

ok( cleanup_column_family('AECTestCF'), "Test column family cleaned up" );
ok( cleanup_keyspace($host, $port), "Test keyspace cleaned up" );
