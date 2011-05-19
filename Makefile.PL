use strict;

use ExtUtils::MakeMaker qw(WriteMakefile);

WriteMakefile(
    NAME              => 'AnyEvent::Cassandra',
    VERSION_FROM      => 'lib/AnyEvent/Cassandra.pm',
    PREREQ_PM         => {
        'AnyEvent'     => 0,
        'Bit::Vector'  => 0,
        'Data::Dump'   => 0,
        'Scalar::Util' => 0,
        'Thrift::XS'   => 1.03,
        'Time::HiRes'  => 0,
    },
    ABSTRACT_FROM     => 'lib/AnyEvent/Cassandra.pm',
    AUTHOR            => 'Andy Grundman <andy@hybridized.org>',
    META_MERGE        => {
        resources => {
            license    => 'http://www.apache.org/licenses/LICENSE-2.0',
            repository => 'https://github.com/andygrundman/anyevent-cassandra',
        },
    },
);
