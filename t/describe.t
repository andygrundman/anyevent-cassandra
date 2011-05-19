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
    ? ( tests => 2 )
    : ( skip_all => "No local Cassandra server found on ${host}:${port}, please start one or set CASSANDRA_HOST and CASSANDRA_PORT" );

ok( setup_keyspace($host, $port), "Test keyspace created" );

my $client = get_client();

# XXX tests here

ok( cleanup_keyspace($host, $port), "Test keyspace cleaned up" );
