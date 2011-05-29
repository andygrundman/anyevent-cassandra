package AECTestUtil;

use strict;
use base qw(Exporter);

use AnyEvent;
use AnyEvent::Cassandra;
use IO::Socket::INET;

use constant TEST_KEYSPACE_NAME => 'AECTestKeyspace';

our @EXPORT = qw(check_server get_client setup_keyspace cleanup_keyspace setup_column_family cleanup_column_family);

my $client;

sub check_server {
    # Look for a local Cassandra server
    my $host = $ENV{CASSANDRA_HOST} || '127.0.0.1';
    my $port = $ENV{CASSANDRA_PORT} || 9160;
    if ( check_port($host, $port) != 1 ) {
        return (0, $host, $port);
    }
    else {
        return (1, $host, $port);
    }
}

sub check_port {
    my ( $host, $port ) = @_;

    my $remote = IO::Socket::INET->new(
        Proto    => "tcp",
        PeerAddr => $host,
        PeerPort => $port
    );
    if ($remote) {
        close $remote;
        return 1;
    }
    else {
        return 0;
    }
}

sub get_client {
    return $client if defined $client;
    
    my ($host, $port) = @_;
    
    $client = AnyEvent::Cassandra->new(
    	host       => "${host}:${port}",
    	timeout    => 5,
    	debug      => 0,
    );
    
    my ($ok, $error) = $client->connect;
    if ( !$ok ) {
        die "Unable to connect to server on ${host}:${port}: $error\n";
    }
    
    return $client;
}

sub setup_keyspace {
    my ($host, $port) = @_;
    
    my $c = get_client($host, $port);
    
    my ($ok, $res) = $c->system_add_keyspace( {
        name               => TEST_KEYSPACE_NAME,
        strategy_class     => 'org.apache.cassandra.locator.SimpleStrategy',
        replication_factor => 1,
        cf_defs            => [],
    } );
    
    if ( !$ok && $res->why =~ /already exists/ ) {
        cleanup_keyspace();
        return setup_keyspace($host, $port);
    }
    
    if ( !$ok ) {
        die "Unable to create test keyspace: " . $res->why . "\n";
    }
    
    ($ok, $res) = $c->set_keyspace(TEST_KEYSPACE_NAME);
    if ( !$ok ) {
        die "Unable to set keyspace to " . TEST_KEYSPACE_NAME . "\n";
    }
    
    return 1;
}

sub cleanup_keyspace {
    my $c = get_client();
    
    my ($ok, $res) = $c->system_drop_keyspace( TEST_KEYSPACE_NAME );
    if ( !$ok ) {
        die "Unable to drop test keyspace: " . $res->why . "\n";
    }
    
    $client = undef;
    return 1;
}

sub setup_column_family {
    my $cf = shift;
    
    my $c = get_client();
    
    my ($ok, $res) = $c->system_add_column_family( ref $cf eq 'HASH' ? $cf : {
        keyspace => TEST_KEYSPACE_NAME,
        name     => $cf,
    } );
    
    if ( !$ok ) {
        die "Unable to create test column family $cf: " . $res->why . "\n";
    }
    
    return 1;
}

sub cleanup_column_family {
    my $cf = shift;
    
    my $c = get_client();
    
    my ($ok, $res) = $c->system_drop_column_family($cf);
    if ( !$ok ) {
        die "Unable to drop test column family $cf: " . $res->why . "\n";
    }
    
    return 1;
    
}

1;
