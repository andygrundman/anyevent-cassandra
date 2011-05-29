# Tests for data methods
#
# TODO:
# get_slice
# multiget_slice
# multiget_count
# get_range_slices
# get_indexed_slices
# add
# remove
# remove_counter
# batch_mutate
# truncate
#
# insert_simple (sugar method)

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
    ? ( tests => 12 )
    : ( skip_all => "No local Cassandra server found on ${host}:${port}, please start one or set CASSANDRA_HOST and CASSANDRA_PORT" );

ok( setup_keyspace($host, $port), "Test keyspace created" );

my $client = get_client();

# A simple user CF
ok( setup_column_family( {
    keyspace        => 'AECTestKeyspace',
    name            => 'AECTestCF_users',
	comparator_type => 'AsciiType',
	column_metadata => [
    	{ name => 'id',       validation_class => 'IntegerType' },
    	{ name => 'email',    validation_class => 'AsciiType' },
    	{ name => 'name',     validation_class => 'UTF8Type' },
    	{ name => 'password', validation_class => 'AsciiType' },
    ],
} ), "Test users column family created" );

# insert
{
    my ($ok, $res) = $client->insert( [
        'foo@bar.com',
        { column_family => 'AECTestCF_users' },
        { name => 'name', value => 'Foo Bar', timestamp => time() },
    ] );
    is( $ok, 1, '[sync ] insert Foo Bar ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->insert( [
        'john@doe.com',
        { column_family => 'AECTestCF_users' },
        { name => 'name', value => 'John Doe', timestamp => time() },
    ], sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] insert John Doe ok' );
        $cv->send;
    } );
    $cv->recv;
}

# get
{
    my ($ok, $res) = $client->get( [
        'foo@bar.com',
        { column_family => 'AECTestCF_users', column => 'name' },
    ] );
    is( $ok, 1, '[sync ] get ok' );
    is( $res->column->value, 'Foo Bar', '[sync ] get column value Foo Bar ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->get( [
        'john@doe.com',
        { column_family => 'AECTestCF_users', column => 'name' },
    ], sub {
        my ($ok, $res) = @_;
        is( $ok, 1, '[async] get ok' );
        is( $res->column->value, 'John Doe', '[async] get column value John Doe ok' );
        $cv->send;
    } );
    $cv->recv;
}

# get_count
{
    my ($ok, $res) = $client->get_count( [
        'foo@bar.com',
        { column_family => 'AECTestCF_users' },
        { column_names => [ 'name' ] },
    ] );
    is( $res, 1, '[sync ] get_count ok' );
}
{
    my $cv = AnyEvent->condvar;
    $client->get_count( [
        'john@doe.com',
        { column_family => 'AECTestCF_users' },
        { column_names => [ 'name' ] },
    ], sub {
        my ($ok, $res) = @_;
        is( $res, 1, '[async] get_count ok' );
        $cv->send;
    } );
    $cv->recv;
}

ok( cleanup_column_family('AECTestCF_users'), "Test users column family cleaned up" );
ok( cleanup_keyspace($host, $port), "Test keyspace cleaned up" );
